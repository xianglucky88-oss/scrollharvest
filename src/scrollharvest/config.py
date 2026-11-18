"""单一职责：集中管理路径、规则版本与默认预算。

规则版本一旦发布即不可修改，只能新增版本；历史结果始终带着产生它的
规则版本号保存，保证换版后仍可解释。
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures" / "input.json"
DB_PATH = ROOT / "scrollharvest.db"

# ---- 身份/内容规则版本（只增不改）-------------------------------------
IDENTITY_RULE_V1 = "key.v1"
FP_V1 = "fp.v1"          # 内容指纹 v1：排除抓取期易变键
FP_V2 = "fp.v2"          # 内容指纹 v2：在 v1 基础上再排除服务端触碰时间
FP_RULE_VERSIONS = (FP_V1, FP_V2)
DEFAULT_FP_VERSION = FP_V1

# ---- 四协议标识 ---------------------------------------------------------
PROTO_OFFSET = "offset"
PROTO_CURSOR = "cursor"
PROTO_SCROLL = "scroll"
PROTO_TIME_ANCHOR = "time_anchor"
ALL_PROTOCOLS = (PROTO_OFFSET, PROTO_CURSOR, PROTO_SCROLL, PROTO_TIME_ANCHOR)

# ---- 默认预算与分页参数 -------------------------------------------------
DEFAULT_PAGE_SIZE = 500
MAX_PAGE_SIZE = 5000
DEFAULT_MAX_PAGES = 200
DEFAULT_MAX_RUNTIME_SECONDS = 30 * 60
DEFAULT_MAX_BYTES = 256 * 1024 * 1024
# 无稳定快照时，连续多少个空页/静止轮次也不能宣告完整
DEFAULT_EMPTY_STREAK_LIMIT = 3
DEFAULT_QUIET_POLLS = 2

# ---- 覆盖范围状态 -------------------------------------------------------
COVERAGE_COVERED = "covered"          # 已确认遍历并落库
COVERAGE_PENDING = "pending"          # 待补（边界漂移/中断/游标失效）
COVERAGE_UNCONFIRMABLE = "unconfirmable"  # 无法确认（源站不提供可验证依据）
