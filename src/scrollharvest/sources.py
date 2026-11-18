"""单一职责：分页数据源抽象与离线可变异内存源。

``InMemoryMutableSource`` 在进程内模拟真实列表源：
- 支持 offset / cursor / scroll / time_anchor 四种协议，可声明只支持其中几种；
- insert/update/delete 会提升结构代际（generation），默认使旧 cursor/scroll 失效；
- cursor/scroll 带 TTL，超时同样失效；
- 支持稳定快照模式（has_stable_snapshot）：首次请求固定视图，之后变异对本轮不可见；
- 删除以显式事件（Deletion）交付——记录在页中“缺席”永远不被解释为删除。

全程离线、确定性，供引擎与测试使用；生产 HTTP 源可实现同一 ListSource 接口。
"""
from __future__ import annotations

import base64
import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone

from .clock import Clock, SystemClock
from .config import (
    PROTO_CURSOR, PROTO_OFFSET, PROTO_SCROLL, PROTO_TIME_ANCHOR,
)
from .errors import CursorExpired, ProtocolUnsupported
from .models import Deletion, Page, PageRequest
from .normalizer import canonical_json


def _b64e(payload: dict) -> str:
    return base64.urlsafe_b64encode(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).decode("ascii")


def _b64d(token: str) -> dict:
    return json.loads(base64.urlsafe_b64decode(token.encode("ascii")))


def _parse_ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


class ListSource(ABC):
    """列表源接口。实现类只需给出名称、协议能力和一次同步取页。"""

    name: str = "abstract"
    supported_protocols: frozenset[str] = frozenset()
    has_stable_snapshot: bool = False

    @abstractmethod
    def fetch(self, request: PageRequest) -> Page:
        """按请求取下一页；失效抛 CursorExpired，不支持抛 ProtocolUnsupported。"""


class InMemoryMutableSource(ListSource):
    def __init__(
        self,
        name: str,
        seed: list[dict],
        supported_protocols: frozenset[str] | tuple[str, ...] | None = None,
        has_stable_snapshot: bool = False,
        cursor_ttl_seconds: int = 300,
        expire_on_generation: bool = True,
        clock: Clock | None = None,
    ):
        self.name = name
        self.supported_protocols = frozenset(
            supported_protocols
            if supported_protocols is not None
            else (PROTO_OFFSET, PROTO_CURSOR, PROTO_SCROLL, PROTO_TIME_ANCHOR)
        )
        self.has_stable_snapshot = has_stable_snapshot
        self.cursor_ttl_seconds = cursor_ttl_seconds
        self.expire_on_generation = expire_on_generation
        self.clock = clock or SystemClock()

        self._items: dict[str, dict] = {}
        for rec in seed:
            r = dict(rec)
            r.setdefault("updated_at", r.get("ts", self.clock.now_iso()))
            self._items[r["id"]] = r
        self.generation = 1
        self._deletions: list[Deletion] = []
        self._delivered_deletions: set[int] = set()
        self._pinned_view: dict[str, dict] | None = None  # 稳定快照固定视图

    # ---- 变异操作（模拟源站并发写入）-----------------------------------
    def insert(self, record: dict) -> None:
        r = dict(record)
        r.setdefault("updated_at", self.clock.now_iso())
        if "id" not in r:
            raise ValueError("seed source requires explicit id")
        self._items[r["id"]] = r
        self.generation += 1

    def update(self, identity: str, changes: dict) -> dict:
        if identity not in self._items:
            raise KeyError(identity)
        rec = dict(self._items[identity])
        rec.update(changes)
        rec["updated_at"] = self.clock.now_iso()
        self._items[identity] = rec
        self.generation += 1
        return rec

    def touch(self, identity: str) -> dict:
        """只刷新服务端触碰时间，业务内容不变。"""
        return self.update(identity, {})

    def delete(self, identity: str) -> Deletion:
        if identity not in self._items:
            raise KeyError(identity)
        del self._items[identity]
        self.generation += 1
        deletion = Deletion(
            identity=identity,
            deleted_at=self.clock.now_iso(),
            source_revision=f"gen-{self.generation}",
        )
        self._deletions.append(deletion)
        return deletion

    # ---- 取页 ------------------------------------------------------------
    def fetch(self, request: PageRequest) -> Page:
        if request.protocol not in self.supported_protocols:
            raise ProtocolUnsupported(
                f"source {self.name!r} does not support {request.protocol}"
            )
        if self.has_stable_snapshot and self._pinned_view is None:
            self._pinned_view = {k: dict(v) for k, v in self._items.items()}
        if request.protocol == PROTO_OFFSET:
            return self._fetch_offset(request)
        if request.protocol == PROTO_CURSOR:
            return self._fetch_cursor(request, scroll=False)
        if request.protocol == PROTO_SCROLL:
            return self._fetch_cursor(request, scroll=True)
        if request.protocol == PROTO_TIME_ANCHOR:
            return self._fetch_time_anchor(request)
        raise ProtocolUnsupported(request.protocol)

    def _visible(self) -> list[dict]:
        """稳定快照下返回 pin 时刻视图；否则返回当前活动记录。"""
        if self._pinned_view is not None:
            return [dict(r) for r in self._pinned_view.values()]
        return [dict(r) for r in self._items.values()]

    def _sorted(self, rows: list[dict]) -> list[dict]:
        return sorted(
            rows,
            key=lambda r: (_parse_ts(r["updated_at"]), r["id"]),
            reverse=True,
        )

    def _due_deletions(self, request: PageRequest) -> list[Deletion]:
        out: list[Deletion] = []
        for idx, d in enumerate(self._deletions):
            if idx in self._delivered_deletions:
                continue
            if request.protocol == PROTO_TIME_ANCHOR and request.anchor:
                # 锚点之后发生的删除不属于本次窗口
                if _parse_ts(d.deleted_at) > _parse_ts(request.anchor):
                    continue
            self._delivered_deletions.add(idx)
            out.append(d)
        return out

    def _make_page(self, rows: list[dict], request: PageRequest,
                   next_request: PageRequest | None) -> Page:
        window = rows[: request.limit]
        payload = canonical_json(window)
        return Page(
            items=window,
            next_request=next_request,
            source_cursor=request.cursor or request.scroll_id,
            deletions=self._due_deletions(request),
            page_index=request.page_index,
            truncated=len(rows) > request.limit,
            bytes_est=len(payload.encode("utf-8")),
        )

    # -- offset -----------------------------------------------------------
    def _fetch_offset(self, request: PageRequest) -> Page:
        rows = self._sorted(self._visible())
        start = request.offset or 0
        remaining = rows[start:]
        nxt = None
        if start + request.limit < len(rows):
            nxt = PageRequest(
                protocol=PROTO_OFFSET, limit=request.limit,
                offset=start + request.limit, page_index=request.page_index + 1,
            )
        return self._make_page(remaining, request, nxt)

    # -- cursor / scroll --------------------------------------------------
    def _check_token(self, token: str | None) -> dict | None:
        if not token:
            return None
        payload = _b64d(token)
        if self.expire_on_generation and payload.get("generation") != self.generation:
            raise CursorExpired(
                f"cursor generation {payload.get('generation')} stale; "
                f"source now at generation {self.generation}"
            )
        issued = _parse_ts(payload["issued_at"])
        if (_parse_ts(self.clock.now_iso()) - issued).total_seconds() > self.cursor_ttl_seconds:
            raise CursorExpired("cursor TTL exceeded")
        return payload

    def _issue_token(self, last: dict | None, *, scroll: bool) -> str:
        return _b64e({
            "kind": "scroll" if scroll else "cursor",
            "generation": self.generation,
            "issued_at": self.clock.now_iso(),
            "last_ts": last["updated_at"] if last else None,
            "last_id": last["id"] if last else None,
        })

    def _fetch_cursor(self, request: PageRequest, *, scroll: bool) -> Page:
        token = request.scroll_id if scroll else request.cursor
        payload = self._check_token(token)
        rows = self._sorted(self._visible())
        if payload and payload.get("last_id") is not None:
            boundary = (payload["last_ts"], payload["last_id"])
            rows = [r for r in rows if (r["updated_at"], r["id"]) < boundary]
        nxt = None
        if len(rows) > request.limit:
            new_token = self._issue_token(rows[request.limit - 1], scroll=scroll)
            if scroll:
                nxt = PageRequest(
                    protocol=PROTO_SCROLL, limit=request.limit,
                    scroll_id=new_token, page_index=request.page_index + 1,
                )
            else:
                nxt = PageRequest(
                    protocol=PROTO_CURSOR, limit=request.limit,
                    cursor=new_token, page_index=request.page_index + 1,
                )
        return self._make_page(rows, request, nxt)

    # -- time anchor ------------------------------------------------------
    def _fetch_time_anchor(self, request: PageRequest) -> Page:
        anchor = request.anchor or self.clock.now_iso()
        rows = self._sorted([
            r for r in self._visible()
            if r["updated_at"] <= anchor
            and (request.floor_anchor is None or r["updated_at"] >= request.floor_anchor)
        ])
        nxt = None
        if len(rows) > request.limit:
            min_ts = min(r["updated_at"] for r in rows[: request.limit])
            nxt = PageRequest(
                protocol=PROTO_TIME_ANCHOR, limit=request.limit,
                anchor=min_ts, floor_anchor=request.floor_anchor,
                scan_dir=request.scan_dir, page_index=request.page_index + 1,
            )
        page = self._make_page(rows, request, nxt)
        page.source_cursor = anchor
        return page
