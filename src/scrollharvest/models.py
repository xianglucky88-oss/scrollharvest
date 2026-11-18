"""单一职责：流水线与采集引擎共用的数据结构。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Record:
    key: str
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class PageRequest:
    """一次分页请求的完整描述，可 JSON 序列化后作为恢复位置持久化。"""
    protocol: str
    limit: int
    offset: int | None = None
    cursor: str | None = None
    scroll_id: str | None = None
    anchor: str | None = None          # 时间锚点（ISO8601 UTC）
    floor_anchor: str | None = None    # 早于该锚点的记录不在本轮范围
    scan_dir: str = "backward"         # time_anchor 的扫描方向
    page_index: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "PageRequest":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class Deletion:
    """源站显式给出的删除事件。注意：记录“没出现在页里”不是删除。"""
    identity: str
    deleted_at: str
    source_revision: str | None = None


@dataclass
class Page:
    """一页响应：数据、下一页请求、服务端游标、显式删除、估算字节数。"""
    items: list[dict[str, Any]]
    next_request: PageRequest | None
    source_cursor: str | None = None
    deletions: list[Deletion] = field(default_factory=list)
    page_index: int = 0
    truncated: bool = False
    bytes_est: int = 0
