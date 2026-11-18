"""单一职责：收敛判定——预算、停止条件、“不能宣告完整”的规则。

核心原则（需求硬约束）：
- 没有稳定源快照时，连续若干空页/静止轮次只能触发静止轮询，
  绝不能据此宣告完整采集；只能以 end_reached_unverified 结束，
  并把未验证范围保留为 pending。
- 预算（页数/时长/字节/静止轮次）触发的是“可恢复的暂停”，不是失败。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .config import (
    DEFAULT_EMPTY_STREAK_LIMIT, DEFAULT_MAX_BYTES, DEFAULT_MAX_PAGES,
    DEFAULT_MAX_RUNTIME_SECONDS, DEFAULT_QUIET_POLLS,
)


class EndReason(str, Enum):
    COMPLETE = "complete"                       # 稳定快照下走到源站结尾
    END_REACHED_UNVERIFIED = "end_reached_unverified"  # 无快照：结尾不可验证
    QUIESCED = "quiesced"                       # 静止轮询通过（仍保留 pending）
    BUDGET_PAGES = "budget_pages"               # 页数预算
    BUDGET_RUNTIME = "budget_runtime"           # 时长预算
    BUDGET_BYTES = "budget_bytes"               # 字节预算
    BUDGET_EMPTY_STREAK = "budget_empty_streak"  # 连续空页上限（暂停补采）
    PROTOCOL_EXHAUSTED = "protocol_exhausted"   # 所有协议均不可用
    FATAL = "fatal"


# 哪些结束原因代表“本轮工作完成、结果可直接使用”
TERMINAL_OK = frozenset({EndReason.COMPLETE, EndReason.END_REACHED_UNVERIFIED,
                         EndReason.QUIESCED})
# 哪些结束原因代表“必须靠断点续采补齐”
RESUMABLE = frozenset({
    EndReason.BUDGET_PAGES, EndReason.BUDGET_RUNTIME, EndReason.BUDGET_BYTES,
    EndReason.BUDGET_EMPTY_STREAK,
})


@dataclass
class HarvestBudget:
    max_pages: int = DEFAULT_MAX_PAGES
    max_runtime_seconds: int = DEFAULT_MAX_RUNTIME_SECONDS
    max_bytes: int = DEFAULT_MAX_BYTES
    empty_streak_limit: int = DEFAULT_EMPTY_STREAK_LIMIT
    quiet_polls: int = DEFAULT_QUIET_POLLS

    def check(self, pages_fetched: int, bytes_fetched: int,
              elapsed_seconds: float, empty_streak: int) -> EndReason | None:
        if pages_fetched >= self.max_pages:
            return EndReason.BUDGET_PAGES
        if elapsed_seconds >= self.max_runtime_seconds:
            return EndReason.BUDGET_RUNTIME
        if bytes_fetched >= self.max_bytes:
            return EndReason.BUDGET_BYTES
        if empty_streak >= self.empty_streak_limit:
            return EndReason.BUDGET_EMPTY_STREAK
        return None

    def enforce(self, pages_fetched: int, bytes_fetched: int,
                elapsed_seconds: float, empty_streak: int) -> None:
        """异常式预算出口：命中即抛 BudgetExceeded（引擎内部使用 check 版本）。"""
        reason = self.check(pages_fetched, bytes_fetched,
                            elapsed_seconds, empty_streak)
        if reason is not None:
            from .errors import BudgetExceeded
            raise BudgetExceeded(reason.value)


@dataclass
class ConvergenceVerdict:
    end_reason: EndReason
    can_claim_complete: bool
    detail: str


def evaluate_tail(*, source_has_snapshot: bool, reached_end: bool,
                  empty_streak: int, quiet_polls_seen: int,
                  budget: HarvestBudget, budget_hit: EndReason | None,
                  protocols_left: int) -> ConvergenceVerdict:
    """在“源站不再给下一页”或预算命中时给出收敛结论。"""
    if protocols_left == 0:
        return ConvergenceVerdict(
            EndReason.PROTOCOL_EXHAUSTED, False,
            "all pagination protocols rejected by source; resume after source change",
        )
    if budget_hit is not None:
        return ConvergenceVerdict(
            budget_hit, False,
            "budget stop: checkpoint retained, resume to continue",
        )
    if reached_end:
        if source_has_snapshot:
            return ConvergenceVerdict(
                EndReason.COMPLETE, True,
                "stable snapshot pinned and source signalled end-of-list",
            )
        return ConvergenceVerdict(
            EndReason.END_REACHED_UNVERIFIED, False,
            "end-of-list seen without stable snapshot; "
            "late/backdated inserts cannot be ruled out",
        )
    if quiet_polls_seen >= budget.quiet_polls:
        return ConvergenceVerdict(
            EndReason.QUIESCED, False,
            "source stayed quiet across polls; pending windows retained",
        )
    return ConvergenceVerdict(
        EndReason.BUDGET_EMPTY_STREAK, False,
        "empty streak limit without evidence of completeness",
    )


def process(record):
    """流水线阶段：逐记录阶段不做收敛判断，保留记录透传。"""
    rec = dict(record)
    rec.setdefault("_stage_convergence", "ok")
    return rec
