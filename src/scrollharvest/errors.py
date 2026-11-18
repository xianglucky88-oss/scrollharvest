"""单一职责：采集流程的领域错误类型。"""


class HarvestError(Exception):
    """采集相关错误的基类。"""


class CursorExpired(HarvestError):
    """游标/scroll 上下文过期或已失效（源站代际变化、TTL 到期）。"""


class ProtocolUnsupported(HarvestError):
    """源站不支持请求的分页协议。"""


class ConfirmationLost(HarvestError):
    """提交已发出但确认丢失，无法判断事务是否落库。"""


class BudgetExceeded(HarvestError):
    """触发预算停止条件（页数/时长/字节）。"""


class FatalHarvestError(HarvestError):
    """不可恢复错误（协议全部不可用、数据无法解析等）。"""
