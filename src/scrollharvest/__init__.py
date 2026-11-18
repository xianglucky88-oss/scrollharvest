"""scrollharvest：四协议分页采集、增量合并与断点续采。"""
from .config import (
    ALL_PROTOCOLS, COVERAGE_COVERED, COVERAGE_PENDING, COVERAGE_UNCONFIRMABLE,
    DEFAULT_FP_VERSION, FP_V1, FP_V2, IDENTITY_RULE_V1,
)
from .convergence import EndReason, HarvestBudget
from .engine import HarvestConfig, HarvestEngine
from .errors import (
    BudgetExceeded, ConfirmationLost, CursorExpired, FatalHarvestError,
    HarvestError, ProtocolUnsupported,
)
from .merge import MergeAction, MergeDecision
from .models import Deletion, Page, PageRequest, Record
from .pagination import (
    CursorPaginator, OffsetPaginator, PaginationPlan, ScrollPaginator,
    TimeAnchorPaginator, build_paginator,
)
from .sources import InMemoryMutableSource, ListSource

__all__ = [
    "ALL_PROTOCOLS", "COVERAGE_COVERED", "COVERAGE_PENDING",
    "COVERAGE_UNCONFIRMABLE", "DEFAULT_FP_VERSION", "FP_V1", "FP_V2",
    "IDENTITY_RULE_V1", "EndReason", "HarvestBudget", "HarvestConfig",
    "HarvestEngine", "BudgetExceeded", "ConfirmationLost", "CursorExpired",
    "FatalHarvestError", "HarvestError", "ProtocolUnsupported", "MergeAction",
    "MergeDecision", "Deletion", "Page", "PageRequest", "Record",
    "CursorPaginator", "OffsetPaginator", "PaginationPlan", "ScrollPaginator",
    "TimeAnchorPaginator", "build_paginator", "InMemoryMutableSource",
    "ListSource",
]
