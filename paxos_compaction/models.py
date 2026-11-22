"""压缩分析所需的数据结构与拒绝原因定义。"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple


class RejectReason(Enum):
    """快照候选或截断请求被拒绝的原因。"""

    SNAPSHOT_INDEX_BEYOND_CHOSEN = (
        "snapshot_index_beyond_chosen",
        "快照边界超过当前 chosen 索引，包含尚未 chosen 的提案",
    )
    CHOSEN_NOT_IN_SNAPSHOT = (
        "chosen_not_in_snapshot",
        "存在已 chosen 但未纳入快照的提案，截断会删除不可丢失的历史",
    )
    TERM_MISMATCH = (
        "term_mismatch",
        "快照记录的 term 与法定人数 acceptor 的 accepted term 不一致",
    )
    STALE_LEADER_TERM = (
        "stale_leader_term",
        "快照由旧 term 的 leader 创建，leader 切换后边界必须重新验证",
    )
    MEMBER_NEEDS_SNAPSHOT = (
        "member_needs_snapshot",
        "成员落后于截断点，其尚未保存快照，截断将使其永久丢失历史",
    )
    MEMBER_INSTALL_FAILED = (
        "member_install_failed",
        "成员的快照安装处于失败状态，恢复完成前不允许截断",
    )
    EMPTY_LOG = ("empty_log", "日志为空，没有可压缩的内容")

    def __init__(self, code: str, description: str) -> None:
        self.code = code
        self.description = description


@dataclass
class LogEntry:
    """一条 Paxos 日志提案。"""

    index: int
    term: int
    command: object


@dataclass
class AcceptorState:
    """acceptor 的 accepted 状态。

    ``accepted_terms`` 记录每个索引上最近一次 accepted 的提案 term，
    压缩后 acceptor 仍可保留这些元数据，用于验证快照边界。
    """

    name: str
    accepted_terms: Dict[int, int] = field(default_factory=dict)
    highest_promised: int = 0

    def accepted_at(self, index: int) -> Optional[int]:
        return self.accepted_terms.get(index)


class InstallState(Enum):
    """成员快照安装状态。"""

    NONE = "none"          # 从未收到快照
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class MemberState:
    """集群成员（learner / follower）的日志位置与快照安装状态。"""

    name: str
    match_index: int = 0
    install_state: InstallState = InstallState.NONE
    snapshot_index: int = 0
    last_installed_chunk: int = -1
    total_chunks: int = 0
    pending_snapshot: Optional["Snapshot"] = None


@dataclass
class Snapshot:
    """一份快照及其内容边界。

    ``last_included_index`` 即快照内容边界：该索引及其之前的日志都
    已被快照覆盖，截断只能删除 ``<= last_included_index`` 的条目。
    """

    last_included_index: int
    last_included_term: int
    created_term: int
    content: object = None


@dataclass
class SnapshotCandidate:
    """快照候选评估结果。"""

    last_included_index: int
    last_included_term: Optional[int]
    valid: bool
    reject_reasons: List[RejectReason] = field(default_factory=list)


@dataclass
class CatchUpTask:
    """落后成员的追赶计划。"""

    member: str
    mode: str                 # "install_snapshot" / "send_entries" / "resume_install"
    target_snapshot_index: int
    from_index: int
    to_index: int
    resume_chunk: Optional[int]
    detail: str


@dataclass
class TruncationPlan:
    """一次截断请求的完整分析结论。"""

    requested_index: int
    safe: bool
    truncatable_indices: List[int]
    safe_upper_bound: int
    reject_reasons: List[RejectReason] = field(default_factory=list)
    lagging_members: List[str] = field(default_factory=list)
    catch_up_tasks: List[CatchUpTask] = field(default_factory=list)


@dataclass
class CompactionResult:
    """模拟器整体分析输出：快照候选 + 截断计划。"""

    chosen_index: int
    leader_term: int
    quorum_size: int
    snapshot_candidates: List[SnapshotCandidate]
    plan: Optional[TruncationPlan]

    def rejected_codes(self) -> List[str]:
        codes = []
        for candidate in self.snapshot_candidates:
            for reason in candidate.reject_reasons:
                if reason.code not in codes:
                    codes.append(reason.code)
        if self.plan is not None:
            for reason in self.plan.reject_reasons:
                if reason.code not in codes:
                    codes.append(reason.code)
        return codes


def quorum_of(member_count: int) -> int:
    """多数派法定人数。"""

    return member_count // 2 + 1


def term_at_index(index: int,
                  log: List[LogEntry],
                  acceptors: List[AcceptorState]) -> Tuple[Optional[int], bool]:
    """获取某索引上的 term。

    优先从（可能已截断的）日志读取；日志中不存在时回退到 acceptor
    的 accepted 记录。返回 ``(term, from_acceptors)``。
    """

    for entry in log:
        if entry.index == index:
            return entry.term, False
    counts = {}  # type: Dict[int, int]
    for acceptor in acceptors:
        term = acceptor.accepted_at(index)
        if term is not None:
            counts[term] = counts.get(term, 0) + 1
    if not counts:
        return None, True
    winner_term = max(counts, key=lambda key: (counts[key], key))
    return winner_term, True
