"""单一职责：把一页记录与本地已确认状态做增量合并决策。

决策类型（MergeAction）：
- INSERT 新身份首次确认；
- UPDATE 同一身份、内容指纹变化（合法更新，保留修订链，不新建记录）；
- UNCHANGED 同一身份、指纹相同（包括重跑、重复页、字段顺序变化）；
- DELETE 仅由源站显式 Deletion 事件触发，缺席不推断删除；
- SIMILAR 兜底键不同的相似记录：身份不同则独立保留，绝不合并。

所有决策携带身份规则版本与指纹规则版本，历史结果换版后仍可解释。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .config import DEFAULT_FP_VERSION, IDENTITY_RULE_V1
from .fingerprint import fingerprint
from .keyer import stable_key
from .normalizer import (
    TOUCH_KEYS, VOLATILE_KEYS, canonical_json, normalize_record,
)


class MergeAction(str, Enum):
    INSERT = "insert"
    UPDATE = "update"
    UNCHANGED = "unchanged"
    DELETE = "delete"
    SIMILAR = "similar"


@dataclass
class MergeDecision:
    action: MergeAction
    identity: str
    record: dict | None = None
    previous_fingerprint: str | None = None
    fingerprint: str | None = None
    identity_rule: str = IDENTITY_RULE_V1
    fp_rule: str = DEFAULT_FP_VERSION
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "action": self.action.value,
            "identity": self.identity,
            "record": self.record,
            "previous_fingerprint": self.previous_fingerprint,
            "fingerprint": self.fingerprint,
            "identity_rule": self.identity_rule,
            "fp_rule": self.fp_rule,
            "reason": self.reason,
        }


def classify_record(raw: dict, existing: dict | None,
                    fp_version: str = DEFAULT_FP_VERSION) -> MergeDecision:
    """对单条记录做合并判定。existing 为本地当前活动记录（含 _fp）。"""
    rec = normalize_record(raw)
    identity, id_rule = stable_key(rec)
    fp, fp_rule = fingerprint(rec, fp_version)

    if existing is None:
        return MergeDecision(
            MergeAction.INSERT, identity, record=rec,
            fingerprint=fp, identity_rule=id_rule, fp_rule=fp_rule,
            reason="identity first seen",
        )

    prev_fp = existing.get("_fp")
    prev_rule = existing.get("_fp_rule", fp_rule)
    if prev_rule != fp_rule:
        # 规则换版：旧指纹与新指纹不可直接比较。把双方都放到“新规则”下
        # 重算规范化内容体：业务内容一致即 UNCHANGED，并在原因中留痕；
        # 历史修订保留旧规则号，保证换版后仍可解释。
        same_body = _content_body(existing, fp_rule) == _content_body(rec, fp_rule)
        if same_body:
            action, reason = MergeAction.UNCHANGED, \
                f"rule migration {prev_rule}->{fp_rule}: canonical body identical"
        else:
            action, reason = MergeAction.UPDATE, \
                f"rule migration {prev_rule}->{fp_rule}: body differs"
    elif prev_fp == fp:
        action, reason = MergeAction.UNCHANGED, "same fingerprint"
    else:
        action, reason = MergeAction.UPDATE, "content fingerprint changed"

    return MergeDecision(
        action, identity, record=rec,
        previous_fingerprint=prev_fp, fingerprint=fp,
        identity_rule=id_rule, fp_rule=fp_rule, reason=reason,
    )


def _content_body(rec: dict, fp_rule: str) -> str:
    exclude = VOLATILE_KEYS | (TOUCH_KEYS if fp_rule.endswith("v2") else frozenset())
    return canonical_json({
        k: v for k, v in rec.items()
        if k not in exclude and not str(k).startswith("_")
    })


def decide_deletion(deletion, existing: dict | None) -> MergeDecision:
    """显式删除事件：存在才产生 DELETE，不存在则幂等忽略。"""
    if existing is None:
        return MergeDecision(
            MergeAction.UNCHANGED, deletion.identity,
            reason="delete event for unknown/already-deleted identity",
        )
    return MergeDecision(
        MergeAction.DELETE, deletion.identity,
        previous_fingerprint=existing.get("_fp"),
        reason=f"explicit source deletion at {deletion.deleted_at}",
    )


def process(record):
    """流水线阶段占位：逐记录流水线不掌握本地状态，合并判定在引擎内完成。"""
    return dict(record)
