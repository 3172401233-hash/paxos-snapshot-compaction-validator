"""压缩安全点的纯函数式分析器。

分析器不持有可变状态，所有结论都基于调用时传入的集群快照计算，
因此同一个分析器可以在 leader 切换后反复重新验证边界。
"""

from typing import Dict, List, Optional, Sequence

from .models import (
    AcceptorState,
    CatchUpTask,
    InstallState,
    LogEntry,
    MemberState,
    RejectReason,
    Snapshot,
    SnapshotCandidate,
    TruncationPlan,
    quorum_of,
    term_at_index,
)


class CompactionAnalyzer:
    """根据 acceptor / learner 状态计算压缩安全点。"""

    def __init__(self,
                 acceptors: Sequence[AcceptorState],
                 members: Sequence[MemberState],
                 chosen_index: int,
                 leader_term: int) -> None:
        self.acceptors = list(acceptors)
        self.members = list(members)
        self.chosen_index = chosen_index
        self.leader_term = leader_term
        self.quorum_size = quorum_of(len(self.acceptors))

    # ------------------------------------------------------------------
    # 法定人数判定
    # ------------------------------------------------------------------
    def _term_has_quorum(self, index: int, term: Optional[int]) -> bool:
        """该索引上的 term 是否得到多数派 acceptor 的 accepted 记录支持。"""

        if term is None:
            return False
        votes = sum(
            1 for acceptor in self.acceptors
            if acceptor.accepted_at(index) == term
        )
        return votes >= self.quorum_size

    def _term_quorum_at(self, index: int) -> Optional[int]:
        """返回某索引上获得法定人数支持的 term，不存在则返回 None。"""

        counts = {}  # type: Dict[int, int]
        for acceptor in self.acceptors:
            term = acceptor.accepted_at(index)
            if term is not None:
                counts[term] = counts.get(term, 0) + 1
        for term, votes in counts.items():
            if votes >= self.quorum_size:
                return term
        return None

    # ------------------------------------------------------------------
    # 快照候选
    # ------------------------------------------------------------------
    def evaluate_snapshot(self,
                          snapshot: Snapshot,
                          log: Sequence[LogEntry]) -> SnapshotCandidate:
        """验证一份快照的内容边界是否安全。"""

        reasons = []  # type: List[RejectReason]
        boundary = snapshot.last_included_index

        if boundary > self.chosen_index:
            reasons.append(RejectReason.SNAPSHOT_INDEX_BEYOND_CHOSEN)

        actual_term, _ = term_at_index(boundary, list(log), self.acceptors)
        if actual_term is None:
            reasons.append(RejectReason.TERM_MISMATCH)
        elif actual_term != snapshot.last_included_term:
            reasons.append(RejectReason.TERM_MISMATCH)
        elif not self._term_has_quorum(boundary, actual_term):
            reasons.append(RejectReason.TERM_MISMATCH)

        if snapshot.created_term != self.leader_term:
            reasons.append(RejectReason.STALE_LEADER_TERM)

        return SnapshotCandidate(
            last_included_index=boundary,
            last_included_term=snapshot.last_included_term,
            valid=not reasons,
            reject_reasons=reasons,
        )

    def snapshot_candidates(self,
                            log: Sequence[LogEntry]) -> List[SnapshotCandidate]:
        """枚举所有安全的快照候选边界。

        安全边界必须满足：
        1. 不超过 chosen 索引（不纳入未 chosen 的提案）；
        2. 该索引上的 term 得到多数派 acceptor 支持；
        3. 不存在“已 chosen 但无法重建”的空洞，即 1..boundary
           每个索引都能从日志或 acceptor 记录中确定 chosen term。
        """

        candidates = []  # type: List[SnapshotCandidate]
        for boundary in range(1, self.chosen_index + 1):
            reasons = []  # type: List[RejectReason]
            quorum_term = self._term_quorum_at(boundary)
            stored_term, _ = term_at_index(boundary, list(log), self.acceptors)
            if quorum_term is None or stored_term is None or quorum_term != stored_term:
                reasons.append(RejectReason.TERM_MISMATCH)
            for index in range(1, boundary + 1):
                term, _ = term_at_index(index, list(log), self.acceptors)
                if term is None or not self._term_has_quorum(index, term):
                    reasons.append(RejectReason.CHOSEN_NOT_IN_SNAPSHOT)
                    break
            candidates.append(SnapshotCandidate(
                last_included_index=boundary,
                last_included_term=quorum_term,
                valid=not reasons,
                reject_reasons=reasons,
            ))
        return candidates

    # ------------------------------------------------------------------
    # 成员安全与追赶
    # ------------------------------------------------------------------
    def _member_safe_for_cut(self,
                             member: MemberState,
                             cut_index: int) -> List[RejectReason]:
        """截断到 cut_index 时，该成员是否会丢失尚未保存的历史。"""

        reasons = []  # type: List[RejectReason]
        durable = max(member.snapshot_index, member.match_index)
        if durable < cut_index:
            if member.install_state == InstallState.FAILED:
                reasons.append(RejectReason.MEMBER_INSTALL_FAILED)
            else:
                reasons.append(RejectReason.MEMBER_NEEDS_SNAPSHOT)
        elif member.install_state == InstallState.FAILED and \
                member.snapshot_index < cut_index:
            reasons.append(RejectReason.MEMBER_INSTALL_FAILED)
        return reasons

    def _catch_up_task(self,
                       member: MemberState,
                       cut_index: int,
                       chosen_index: int) -> CatchUpTask:
        """为落后成员生成追赶计划。"""

        if member.install_state == InstallState.FAILED:
            resume_chunk = member.last_installed_chunk + 1
            return CatchUpTask(
                member=member.name,
                mode="resume_install",
                target_snapshot_index=cut_index,
                from_index=member.snapshot_index,
                to_index=cut_index,
                resume_chunk=resume_chunk,
                detail=(
                    "从块 %d 断点续传快照至边界 %d，安装完成前禁止截断"
                    % (resume_chunk, cut_index)
                ),
            )
        if member.install_state == InstallState.IN_PROGRESS:
            resume_chunk = member.last_installed_chunk + 1
            return CatchUpTask(
                member=member.name,
                mode="resume_install",
                target_snapshot_index=cut_index,
                from_index=member.snapshot_index,
                to_index=cut_index,
                resume_chunk=resume_chunk,
                detail=(
                    "快照安装进行中，从块 %d 续传到边界 %d"
                    % (resume_chunk, cut_index)
                ),
            )
        if member.match_index < cut_index:
            return CatchUpTask(
                member=member.name,
                mode="install_snapshot",
                target_snapshot_index=cut_index,
                from_index=member.match_index,
                to_index=cut_index,
                resume_chunk=None,
                detail=(
                    "成员日志位置 %d 早于截断点 %d，需先安装快照"
                    % (member.match_index, cut_index)
                ),
            )
        return CatchUpTask(
            member=member.name,
            mode="send_entries",
            target_snapshot_index=cut_index,
            from_index=member.match_index + 1,
            to_index=chosen_index,
            resume_chunk=None,
            detail=(
                "快照已就位，追加同步日志条目 %d..%d"
                % (member.match_index + 1, chosen_index)
            ),
        )

    # ------------------------------------------------------------------
    # 截断计划
    # ------------------------------------------------------------------
    def plan_truncation(self,
                        snapshot: Snapshot,
                        log: Sequence[LogEntry]) -> TruncationPlan:
        """计算在给定快照边界下的可截断范围与追赶计划。"""

        candidate = self.evaluate_snapshot(snapshot, log)
        cut_index = snapshot.last_included_index
        reasons = list(candidate.reject_reasons)

        present_indices = sorted(entry.index for entry in log
                                 if entry.index <= cut_index)

        lagging = []  # type: List[str]
        tasks = []    # type: List[CatchUpTask]
        for member in self.members:
            member_reasons = self._member_safe_for_cut(member, cut_index)
            for reason in member_reasons:
                if reason not in reasons:
                    reasons.append(reason)
            durable = max(member.snapshot_index, member.match_index)
            if durable < cut_index or member.install_state in (
                    InstallState.FAILED, InstallState.IN_PROGRESS):
                lagging.append(member.name)
                tasks.append(self._catch_up_task(
                    member, cut_index, self.chosen_index))

        blocking = {
            RejectReason.MEMBER_NEEDS_SNAPSHOT,
            RejectReason.MEMBER_INSTALL_FAILED,
            RejectReason.SNAPSHOT_INDEX_BEYOND_CHOSEN,
            RejectReason.TERM_MISMATCH,
            RejectReason.STALE_LEADER_TERM,
            RejectReason.CHOSEN_NOT_IN_SNAPSHOT,
        }
        safe = not any(reason in blocking for reason in reasons)

        if safe:
            truncatable = present_indices
            upper_bound = cut_index
        else:
            # 即使请求被拒绝，也给出“当前状态下真正安全”的上界，
            # 即所有成员都已持久化（日志或完整快照）的最大 chosen 索引。
            upper_bound = min(
                [self.chosen_index] +
                [max(m.snapshot_index, m.match_index) for m in self.members]
            )
            upper_bound = min(upper_bound, self.chosen_index)
            truncatable = sorted(
                index for index in present_indices if index <= upper_bound
            )

        return TruncationPlan(
            requested_index=cut_index,
            safe=safe,
            truncatable_indices=truncatable,
            safe_upper_bound=upper_bound,
            reject_reasons=reasons,
            lagging_members=lagging,
            catch_up_tasks=tasks,
        )

    def revalidate_after_leader_change(self,
                                       snapshot: Snapshot,
                                       log: Sequence[LogEntry],
                                       new_leader_term: int
                                       ) -> SnapshotCandidate:
        """leader 切换后用新 term 重新验证快照边界。"""

        swung = CompactionAnalyzer(
            self.acceptors, self.members, self.chosen_index, new_leader_term
        )
        return swung.evaluate_snapshot(snapshot, log)
