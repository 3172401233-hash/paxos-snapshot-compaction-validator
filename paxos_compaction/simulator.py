"""有状态的 Paxos 压缩模拟器。

模拟器在内存中维护 acceptor accepted 状态、成员日志位置、快照安装
状态与 leader term，用于推演截断竞争、leader 切换、安装失败与
learner 断点续传等场景。它不实现任何网络通信：所有“消息”都是
本地方法调用。
"""

from typing import Dict, List, Optional, Sequence

from .analyzer import CompactionAnalyzer
from .models import (
    AcceptorState,
    CompactionResult,
    InstallState,
    LogEntry,
    MemberState,
    RejectReason,
    Snapshot,
    SnapshotCandidate,
    TruncationPlan,
)


class HistoryUnavailableError(RuntimeError):
    """日志已被截断且成员没有覆盖该位置的快照，无法继续追加同步。"""


class CompactionSimulator:
    """内存版 Paxos 日志压缩集群模拟器。"""

    def __init__(self,
                 acceptor_names: Sequence[str],
                 member_names: Sequence[str]) -> None:
        self.acceptors = {
            name: AcceptorState(name=name) for name in acceptor_names
        }
        self.members = {
            name: MemberState(name=name) for name in member_names
        }
        self.log = []  # type: List[LogEntry]
        self.chosen_index = 0
        self.leader_term = 1
        self.truncated_up_to = 0
        self.snapshots = []  # type: List[Snapshot]
        self._quorum = len(acceptor_names) // 2 + 1

    # ------------------------------------------------------------------
    # 提案与 chosen
    # ------------------------------------------------------------------
    def propose(self, command: object) -> LogEntry:
        """leader 接受一个新提案并在法定人数上形成 chosen。"""

        index = len(self.log) + 1 + self.truncated_up_to
        entry = LogEntry(index=index, term=self.leader_term, command=command)
        self.log.append(entry)
        votes = 0
        for acceptor in self.acceptors.values():
            acceptor.accepted_terms[index] = self.leader_term
            acceptor.highest_promised = max(
                acceptor.highest_promised, self.leader_term
            )
            votes += 1
            if votes >= self._quorum:
                break
        self.chosen_index = index
        return entry

    def entry_term(self, index: int) -> Optional[int]:
        for entry in self.log:
            if entry.index == index:
                return entry.term
        for acceptor in self.acceptors.values():
            term = acceptor.accepted_at(index)
            if term is not None:
                return term
        return None

    # ------------------------------------------------------------------
    # 快照与截断
    # ------------------------------------------------------------------
    def create_snapshot(self,
                        boundary: int,
                        content: object = None) -> Snapshot:
        """在 chosen 日志上创建快照，边界必须不超过 chosen 索引。"""

        if boundary <= 0 or boundary > self.chosen_index:
            raise ValueError(
                "快照边界 %d 超出 chosen 索引 %d"
                % (boundary, self.chosen_index)
            )
        term = self.entry_term(boundary)
        if term is None:
            raise ValueError("索引 %d 的 term 无法确定" % boundary)
        snapshot = Snapshot(
            last_included_index=boundary,
            last_included_term=term,
            created_term=self.leader_term,
            content=content,
        )
        self.snapshots.append(snapshot)
        return snapshot

    def latest_snapshot(self) -> Optional[Snapshot]:
        return self.snapshots[-1] if self.snapshots else None

    def _analyzer(self) -> CompactionAnalyzer:
        return CompactionAnalyzer(
            acceptors=list(self.acceptors.values()),
            members=list(self.members.values()),
            chosen_index=self.chosen_index,
            leader_term=self.leader_term,
        )

    def analyze(self,
                snapshot: Optional[Snapshot] = None) -> CompactionResult:
        """输出快照候选与（针对指定/最新快照的）截断计划。"""

        analyzer = self._analyzer()
        candidates = analyzer.snapshot_candidates(self.log)
        target = snapshot if snapshot is not None else self.latest_snapshot()
        plan = analyzer.plan_truncation(target, self.log) if target else None
        return CompactionResult(
            chosen_index=self.chosen_index,
            leader_term=self.leader_term,
            quorum_size=self._quorum,
            snapshot_candidates=candidates,
            plan=plan,
        )

    def truncate(self,
                 snapshot: Optional[Snapshot] = None) -> TruncationPlan:
        """安全时执行截断；不安全时拒绝并保留全部日志。"""

        target = snapshot if snapshot is not None else self.latest_snapshot()
        if target is None:
            raise ValueError("没有可用于截断的快照")
        plan = self._analyzer().plan_truncation(target, self.log)
        if not plan.safe:
            return plan
        boundary = target.last_included_index
        self.log = [entry for entry in self.log if entry.index > boundary]
        self.truncated_up_to = boundary
        for member in self.members.values():
            member.snapshot_index = max(member.snapshot_index, boundary)
            if member.match_index < boundary:
                member.match_index = boundary
        return plan

    # ------------------------------------------------------------------
    # learner 快照安装（分块、失败、断点续传）
    # ------------------------------------------------------------------
    def begin_snapshot_install(self,
                               member_name: str,
                               snapshot: Snapshot,
                               total_chunks: int) -> MemberState:
        if total_chunks <= 0:
            raise ValueError("total_chunks 必须为正整数")
        member = self.members[member_name]
        member.install_state = InstallState.IN_PROGRESS
        member.snapshot_index = 0
        member.last_installed_chunk = -1
        member.total_chunks = total_chunks
        member.pending_snapshot = snapshot
        return member

    def deliver_snapshot_chunk(self,
                               member_name: str,
                               chunk_seq: int) -> MemberState:
        member = self.members[member_name]
        if member.install_state != InstallState.IN_PROGRESS:
            raise RuntimeError(
                "成员 %s 当前状态 %s 不接受快照块"
                % (member_name, member.install_state.value)
            )
        expected = member.last_installed_chunk + 1
        if chunk_seq != expected:
            raise ValueError(
                "块序号乱序：期望 %d，收到 %d" % (expected, chunk_seq)
            )
        member.last_installed_chunk = chunk_seq
        if chunk_seq == member.total_chunks - 1:
            pending = member.pending_snapshot
            assert pending is not None
            member.install_state = InstallState.COMPLETE
            member.snapshot_index = pending.last_included_index
            member.pending_snapshot = None
        return member

    def fail_snapshot_install(self, member_name: str) -> MemberState:
        member = self.members[member_name]
        if member.install_state != InstallState.IN_PROGRESS:
            raise RuntimeError("只有进行中的安装可以标记失败")
        member.install_state = InstallState.FAILED
        return member

    def resume_snapshot_install(self, member_name: str) -> int:
        """断点续传：返回下一个需要重传的块序号。"""

        member = self.members[member_name]
        if member.install_state != InstallState.FAILED:
            raise RuntimeError("只有失败的安装可以续传")
        member.install_state = InstallState.IN_PROGRESS
        return member.last_installed_chunk + 1

    # ------------------------------------------------------------------
    # learner 日志追加同步
    # ------------------------------------------------------------------
    def send_entries(self,
                     member_name: str,
                     to_index: Optional[int] = None) -> List[LogEntry]:
        """向成员追加日志；历史已截断且无快照覆盖时抛出异常。"""

        member = self.members[member_name]
        from_index = member.match_index + 1
        if from_index <= self.truncated_up_to and \
                member.snapshot_index < self.truncated_up_to:
            raise HistoryUnavailableError(
                "成员 %s 需要索引 %d，但日志已截断到 %d 且无覆盖快照"
                % (member_name, from_index, self.truncated_up_to)
            )
        if member.snapshot_index > member.match_index:
            member.match_index = member.snapshot_index
            from_index = member.match_index + 1
        upper = self.chosen_index if to_index is None else min(
            to_index, self.chosen_index
        )
        sent = []  # type: List[LogEntry]
        for entry in self.log:
            if from_index <= entry.index <= upper:
                sent.append(entry)
        if sent:
            member.match_index = sent[-1].index
        return sent

    # ------------------------------------------------------------------
    # leader 切换与边界重验
    # ------------------------------------------------------------------
    def change_leader(self, new_term: int) -> None:
        if new_term <= self.leader_term:
            raise ValueError("新 term 必须严格大于当前 term")
        self.leader_term = new_term

    def revalidate_snapshot(self,
                            snapshot: Optional[Snapshot] = None
                            ) -> SnapshotCandidate:
        """leader 切换后用当前 term 重新验证快照边界。"""

        target = snapshot if snapshot is not None else self.latest_snapshot()
        if target is None:
            raise ValueError("没有可验证的快照")
        return self._analyzer().evaluate_snapshot(target, self.log)

    def revalidate_after_leader_change(self,
                                       new_term: int,
                                       snapshot: Optional[Snapshot] = None
                                       ) -> SnapshotCandidate:
        self.change_leader(new_term)
        return self.revalidate_snapshot(snapshot)

    # ------------------------------------------------------------------
    # 汇总
    # ------------------------------------------------------------------
    def summary(self, result: Optional[CompactionResult] = None) -> Dict[str, object]:
        current = result if result is not None else self.analyze()
        plan = current.plan
        return {
            "chosen_index": current.chosen_index,
            "leader_term": current.leader_term,
            "quorum_size": current.quorum_size,
            "truncated_up_to": self.truncated_up_to,
            "snapshot_candidates": [
                {
                    "index": candidate.last_included_index,
                    "term": candidate.last_included_term,
                    "valid": candidate.valid,
                    "reject": [r.code for r in candidate.reject_reasons],
                }
                for candidate in current.snapshot_candidates
            ],
            "plan": None if plan is None else {
                "requested_index": plan.requested_index,
                "safe": plan.safe,
                "truncatable_indices": plan.truncatable_indices,
                "safe_upper_bound": plan.safe_upper_bound,
                "reject_reasons": [r.code for r in plan.reject_reasons],
                "lagging_members": plan.lagging_members,
                "catch_up_tasks": [
                    {
                        "member": task.member,
                        "mode": task.mode,
                        "from_index": task.from_index,
                        "to_index": task.to_index,
                        "resume_chunk": task.resume_chunk,
                        "detail": task.detail,
                    }
                    for task in plan.catch_up_tasks
                ],
            },
        }
