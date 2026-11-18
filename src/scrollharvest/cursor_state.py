"""单一职责：断点续采状态机——恢复位置、在途请求、覆盖窗口。

状态推进铁律（需求硬约束）：
- 恢复位置只在“整页记录确认入库”之后推进；在途（inflight）请求先落盘再取页，
  进程崩溃后重放该请求，由写台账保证不重复写入。
- 游标失效/协议切换时，旧窗口标记 pending，从新协议首页重走；
  已确认身份通过幂等层短路。
- 覆盖窗口三态：covered / pending / unconfirmable（无快照且无锚点依据）。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum

from .config import (
    COVERAGE_COVERED, COVERAGE_PENDING,
    PROTO_CURSOR, PROTO_OFFSET, PROTO_SCROLL, PROTO_TIME_ANCHOR,
)
from .models import PageRequest


class CheckpointPhase(str, Enum):
    NEW = "new"                 # 尚未开始
    INFLIGHT = "inflight"       # 请求已持久化，等待确认
    CONFIRMED = "confirmed"     # 整页已确认入库，恢复位置已推进
    CURSOR_DEAD = "cursor_dead"  # 游标失效，等待协议回退
    REPAIR = "repair"           # 补采 pending 窗口
    DONE = "done"


@dataclass
class CoverageWindow:
    scope: str                  # 协议下的窗口标识
    protocol: str
    lower: str | None           # offset 数值/时间锚点下界（字符串统一存储）
    upper: str | None
    state: str = COVERAGE_PENDING
    basis: str = ""             # 判定依据（snapshot/cursor/anchor/assumption）
    note: str = ""

    def to_dict(self) -> dict:
        return dict(self.__dict__)


def window_scope(protocol: str, page_index: int, request: PageRequest) -> str:
    if protocol == PROTO_OFFSET:
        start = request.offset or 0
        return f"offset:{start}:{start + request.limit}"
    if protocol == PROTO_TIME_ANCHOR:
        return f"anchor:{request.anchor or 'now'}:p{page_index}"
    if protocol == PROTO_SCROLL:
        return f"scroll:p{page_index}"
    return f"cursor:p{page_index}"


def mark_pending_on_cursor_dead(windows: list[CoverageWindow],
                                protocol: str) -> list[CoverageWindow]:
    """游标/scroll 失效：该协议下未确认窗口全部回退为 pending。"""
    out = []
    for w in windows:
        if w.protocol == protocol and w.state == COVERAGE_COVERED:
            # 已确认入库的记录不丢，但“源站侧覆盖”在新代际下不再可证
            out.append(CoverageWindow(
                w.scope, w.protocol, w.lower, w.upper,
                COVERAGE_PENDING, w.basis,
                note=(w.note + " | cursor invalidated, re-verify").strip(" |"),
            ))
        else:
            out.append(w)
    return out


def unconfirmable_without_snapshot(protocol: str) -> bool:
    """无稳定快照时，任何协议的“源站结尾”窗口都无法证明没有迟到/回填插入。"""
    return protocol in (PROTO_OFFSET, PROTO_SCROLL, PROTO_CURSOR,
                        PROTO_TIME_ANCHOR)


def serialize_request(request: PageRequest | None) -> str | None:
    return json.dumps(request.to_dict(), ensure_ascii=False) if request else None


def deserialize_request(payload: str | None) -> PageRequest | None:
    return PageRequest.from_dict(json.loads(payload)) if payload else None


def process(record):
    """流水线阶段：标注阶段名，真实检查点逻辑在引擎/存储层。"""
    rec = dict(record)
    rec.setdefault("_stage_cursor_state", "naive")
    return rec
