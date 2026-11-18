"""单一职责：派生版本化的稳定身份键。

身份判定优先级：
  1. 源站稳定 id（字符串化）；
  2. 规范化后的 URL（跟踪参数、fragment、大小写差异不产生新身份）；
  3. 业务字段兜底（剔除易变键后做规范化哈希）。

字段顺序变化不影响键；URL 或字段顺序变化不能产生新记录。
规则版本号写入键前缀与修订历史，换版只新增函数，旧键永久可解释。
"""
import hashlib

from .config import IDENTITY_RULE_V1
from .normalizer import canonical_json, canonical_url, VOLATILE_KEYS


def _hash16(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def stable_key(record: dict, rule: str = IDENTITY_RULE_V1) -> tuple[str, str]:
    """返回 (identity, rule_version)。当前只发布 key.v1。"""
    if rule != IDENTITY_RULE_V1:
        raise ValueError(f"unknown identity rule version: {rule}")
    rid = record.get("id")
    if rid not in (None, ""):
        return str(rid), rule
    url = record.get("url") or record.get("href")
    if url:
        return "u:" + _hash16(canonical_url(str(url))), rule
    business = {k: v for k, v in record.items()
                if k not in VOLATILE_KEYS and not str(k).startswith("_")}
    return "h:" + _hash16(canonical_json(business)), rule


def key_of(record):
    """向后兼容的流水线主键（fixtures 使用 id）。"""
    identity, _ = stable_key(record)
    return identity
