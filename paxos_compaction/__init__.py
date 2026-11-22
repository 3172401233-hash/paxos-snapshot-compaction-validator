"""Paxos 快照压缩安全点模拟器。

该包只在内存中推演 Paxos 日志压缩、快照截断与 learner 追赶的
安全性，不实现任何网络通信。
"""

from .models import (
    AcceptorState,
    CatchUpTask,
    CompactionResult,
    InstallState,
    LogEntry,
    MemberState,
    RejectReason,
    Snapshot,
    SnapshotCandidate,
    TruncationPlan,
)
from .analyzer import CompactionAnalyzer
from .simulator import CompactionSimulator, HistoryUnavailableError

__all__ = [
    "AcceptorState",
    "CatchUpTask",
    "CompactionAnalyzer",
    "CompactionResult",
    "CompactionSimulator",
    "HistoryUnavailableError",
    "InstallState",
    "LogEntry",
    "MemberState",
    "RejectReason",
    "Snapshot",
    "SnapshotCandidate",
    "TruncationPlan",
]
