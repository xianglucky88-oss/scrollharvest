"""单一职责：字段规整与规范化（记录变换 + 规范化原语）。

规范化原语 `canonical_json` 供身份键与内容指纹复用，保证字段顺序变化
不会产生新的键或新的指纹（sort_keys + 统一分隔符 + 稳定的 Unicode 编码）。
"""
import json
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# 不参与身份/内容判定的易变键
VOLATILE_KEYS = frozenset({"_stage", "_ingested_at", "_fetch_at", "_run_id"})
# 身份字段：参与身份键，但比较“业务内容是否相同”时剔除，
# 这样不同 id 的两条记录才能被识别为“相似独立记录”
IDENTITY_KEYS = frozenset({"id", "url", "href"})
# 服务端“触碰时间”：不影响业务内容；v2 指纹额外排除
TOUCH_KEYS = frozenset({"ts", "updated_at", "fetched_at"})
# 身份判定时丢弃的 URL 跟踪参数
TRACKING_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "gclid", "fbclid", "ref", "ref_src", "spm",
})


def process(record):
    """流水线阶段：对 fixtures 形态记录做轻量规整（保持向后兼容）。"""
    rec = dict(record)
    rec["src"] = str(rec.get("src", "")).strip().lower()
    rec["name"] = str(rec.get("name", "")).strip()
    rec["value"] = int(rec.get("value", 0))
    return rec


def normalize_record(record: dict) -> dict:
    """引擎入口规整：去首尾空白、统一大小写规则，保留全部业务字段。"""
    rec = {}
    for k, v in record.items():
        if isinstance(v, str):
            rec[k] = v.strip()
        else:
            rec[k] = v
    if "src" in rec:
        rec["src"] = str(rec["src"]).strip().lower()
    return rec


def canonical_json(obj) -> str:
    """确定性序列化：键排序、紧凑分隔、ensure_ascii=False、对嵌套同样生效。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def canonical_url(url: str) -> str:
    """URL 规范化：小写 scheme/host、去 fragment、剔除跟踪参数并排序查询串。

    路径大小写与其余查询参数保留（不同源站语义不同，不做激进改写）。
    """
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    query = urlencode(
        sorted((k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
               if k.lower() not in TRACKING_PARAMS)
    )
    return urlunsplit((scheme, netloc, parts.path, query, ""))
