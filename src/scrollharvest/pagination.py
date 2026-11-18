"""单一职责：分页协议——offset / cursor / 无限滚动(scroll) / 时间锚点。

分页器是纯状态推进器：只根据上一页响应推导下一个 ``PageRequest``，
不接触数据库、不做合并判断。协议切换/游标失效后的“从哪里重走”由
``PaginationPlan`` 与引擎配合完成，策略集中在 plan 中。
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from .config import (
    ALL_PROTOCOLS, PROTO_CURSOR, PROTO_OFFSET, PROTO_SCROLL, PROTO_TIME_ANCHOR,
)
from .errors import ProtocolUnsupported
from .models import Page, PageRequest


class Paginator(ABC):
    protocol: str

    def __init__(self, page_size: int):
        self.page_size = page_size

    @abstractmethod
    def first_request(self) -> PageRequest:
        """本轮/本协议的起点请求。"""

    @abstractmethod
    def next_request(self, page: Page) -> PageRequest | None:
        """优先采用源站自报的 next_request；协议不匹配时由分页器本地推导。"""


class OffsetPaginator(Paginator):
    protocol = PROTO_OFFSET

    def first_request(self) -> PageRequest:
        return PageRequest(protocol=PROTO_OFFSET, limit=self.page_size, offset=0)

    def next_request(self, page: Page) -> PageRequest | None:
        if page.next_request and page.next_request.protocol == PROTO_OFFSET:
            return page.next_request
        # 源站未给下一页：尊重其 end-of-list 信号，不按页码盲推
        if page.next_request is None:
            return None
        req = PageRequest(
            protocol=PROTO_OFFSET, limit=self.page_size,
            offset=(page.page_index + 1) * self.page_size,
            page_index=page.page_index + 1,
        )
        return req if page.items else None


class CursorPaginator(Paginator):
    protocol = PROTO_CURSOR

    def first_request(self) -> PageRequest:
        return PageRequest(protocol=PROTO_CURSOR, limit=self.page_size)

    def next_request(self, page: Page) -> PageRequest | None:
        if page.items and page.next_request and page.next_request.protocol == PROTO_CURSOR:
            return page.next_request
        return None


class ScrollPaginator(Paginator):
    """无限滚动：首屏无上下文，之后必须携带服务端 scroll_id。"""

    protocol = PROTO_SCROLL

    def first_request(self) -> PageRequest:
        return PageRequest(protocol=PROTO_SCROLL, limit=self.page_size)

    def next_request(self, page: Page) -> PageRequest | None:
        if page.items and page.next_request and page.next_request.protocol == PROTO_SCROLL:
            return page.next_request
        return None


class TimeAnchorPaginator(Paginator):
    """时间锚点：从 now/起始锚点按 updated_at 向历史方向翻页。"""

    protocol = PROTO_TIME_ANCHOR

    def __init__(self, page_size: int, start_anchor: str | None = None,
                 floor_anchor: str | None = None):
        super().__init__(page_size)
        self.start_anchor = start_anchor
        self.floor_anchor = floor_anchor  # 早于该锚点的记录不在本轮范围

    def first_request(self) -> PageRequest:
        return PageRequest(
            protocol=PROTO_TIME_ANCHOR, limit=self.page_size,
            anchor=self.start_anchor, floor_anchor=self.floor_anchor,
            scan_dir="backward",
        )

    def next_request(self, page: Page) -> PageRequest | None:
        if not page.items:
            return None
        if page.next_request and page.next_request.protocol == PROTO_TIME_ANCHOR:
            nxt = page.next_request
            nxt.floor_anchor = self.floor_anchor
            if self.floor_anchor and nxt.anchor and nxt.anchor < self.floor_anchor:
                return None
            return nxt
        return None


_PAGINATOR_CLASSES = {
    PROTO_OFFSET: OffsetPaginator,
    PROTO_CURSOR: CursorPaginator,
    PROTO_SCROLL: ScrollPaginator,
    PROTO_TIME_ANCHOR: TimeAnchorPaginator,
}


def build_paginator(protocol: str, page_size: int, **kwargs) -> Paginator:
    cls = _PAGINATOR_CLASSES.get(protocol)
    if cls is None:
        raise ProtocolUnsupported(f"no paginator for protocol {protocol!r}")
    return cls(page_size, **kwargs) if protocol == PROTO_TIME_ANCHOR else cls(page_size)


class PaginationPlan:
    """协议回退顺序与回退起点策略。

    回退顺序默认按“对动态数据健壮性”排列：
    time_anchor → cursor → scroll → offset。
    offset 放最后：动态源下 offset 会因头部插入/删除发生窗口漂移，
    只能配合覆盖范围重走来收敛。
    """

    def __init__(self, preferred_order: tuple[str, ...] = ALL_PROTOCOLS,
                 page_size: int = 500, **paginator_kwargs):
        self.order = preferred_order
        self.page_size = page_size
        self.paginator_kwargs = paginator_kwargs

    def candidates(self, source) -> list[Paginator]:
        """按回退顺序构造分页器。

        不过滤源不支持的协议：保留完整回退链，由源在 fetch 时拒绝
        （ProtocolUnsupported），引擎据此计数并切换到下一协议——
        这样“协议切换”本身可观测、可在运行台账中审计。
        """
        return [
            build_paginator(proto, self.page_size, **self._kw_for(proto))
            for proto in self.order
        ]

    def supported_candidates(self, source) -> list[Paginator]:
        """仅返回源声明支持的分页器（用于能力预检场景）。"""
        return [p for p in self.candidates(source)
                if p.protocol in source.supported_protocols]

    def _kw_for(self, proto: str) -> dict:
        if proto != PROTO_TIME_ANCHOR:
            return {}
        return {k: v for k, v in self.paginator_kwargs.items()
                if k in ("start_anchor", "floor_anchor")}

    @staticmethod
    def fallback_request(paginator: Paginator) -> PageRequest:
        """协议切换/游标失效后的重走起点：新协议的首页请求。

        已确认入库的记录由幂等层去重，因此从头重走不会重复写入；
        覆盖范围表负责记录“哪一段还没在新协议下确认过”。
        """
        return paginator.first_request()
