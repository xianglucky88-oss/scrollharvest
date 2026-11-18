"""单一职责：版本化内容指纹。

- fp.v1：剔除抓取期易变键后对业务内容做规范化哈希（身份字段 id/url 保留，
  同身份更新可被检出）。
- fp.v2：在 fp.v1 基础上再剔除服务端触碰时间（ts/updated_at/fetched_at），
  仅重打时间戳不构成内容变更。

另提供 ``business_body_fingerprint``：剔除身份字段后的业务内容指纹，
供“相似独立记录”检测使用（不同 id、相同业务内容）。

规则只增不改：每个版本的算法永久保留，历史修订带着当时的版本号入库，
换版后可以重新解释历史结果，但不会静默改写历史。
"""
import hashlib

from .config import FP_V1, FP_V2, FP_RULE_VERSIONS
from .normalizer import (
    IDENTITY_KEYS, TOUCH_KEYS, VOLATILE_KEYS, canonical_json,
)


def _content(record: dict, exclude: frozenset) -> dict:
    return {
        k: v for k, v in record.items()
        if k not in exclude and not str(k).startswith("_")
    }


def fingerprint(record: dict, version: str = FP_V1) -> tuple[str, str]:
    """返回 (fingerprint, rule_version)。"""
    if version == FP_V1:
        body = _content(record, VOLATILE_KEYS)
    elif version == FP_V2:
        body = _content(record, VOLATILE_KEYS | TOUCH_KEYS)
    else:
        raise ValueError(
            f"unknown fingerprint rule: {version}; known={FP_RULE_VERSIONS}"
        )
    return hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest(), version


def business_body_fingerprint(record: dict) -> str:
    """剔除身份字段与易变键后的业务内容指纹（用于相似记录检测，不带版本语义）。"""
    body = _content(record, VOLATILE_KEYS | IDENTITY_KEYS)
    return hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()


def process(record):
    """流水线阶段：标注 v1 指纹（保持阶段链可用）。"""
    rec = dict(record)
    fp, ver = fingerprint(rec, FP_V1)
    rec.setdefault("_stage_fingerprint", "naive")
    rec["_fp"] = fp
    rec["_fp_rule"] = ver
    return rec
