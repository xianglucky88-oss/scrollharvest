"""薄编排：loader → normalizer→cursor_state→fingerprint→convergence → keyer → store。

本模块保留为 fixtures 离线摄取的兼容入口（单一职责、不含业务判断）。
动态列表采集的完整分页/合并/续采流程见 engine.HarvestEngine。
"""
import importlib

from . import loader as _loader
from . import store as _store
from .keyer import key_of
from .models import Record

CHAIN_NAMES = ['normalizer', 'cursor_state', 'fingerprint', 'convergence']


def run(con=None):
    own = con is None
    con = con or _store.connect()
    try:
        chain = []
        for n in CHAIN_NAMES:
            mod = importlib.import_module("." + n, __package__)
            chain.append(mod.process)
        fed = 0
        for raw in _loader.load():
            rec = raw
            for fn in chain:
                rec = fn(rec)
            _store.upsert(con, Record(key=key_of(rec), data=rec))
            fed += 1
        _store.commit(con)
        return {"fed": fed, "rows": _store.count(con)}
    finally:
        if own:
            con.close()
