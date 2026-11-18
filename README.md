# scrollharvest — 动态列表的四协议分页采集、增量合并与断点续采

运行栈：Python 3.11+ 标准库 `sqlite3`（离线流程零第三方依赖；测试用 `pytest`）

## 解决什么问题

源站在采集期间持续插入、编辑、删除记录时：

- offset 分页会因头部插入/删除发生窗口漂移，漏行或反复翻到内容相同的页；
- cursor / 无限滚动上下文会因代际变化或 TTL 失效；
- 任务中断后需要从确认点续采，且“提交成功但确认丢失”不能重复写入；
- 没有稳定源快照时，连续空页不能被当作“采集完整”。

本项目把分页、增量合并、断点续采做成完整流程，支持 **offset / cursor /
无限滚动(scroll) / 时间锚点(time_anchor)** 四协议，并在 SQLite 中留下
有效记录、修订来源与每轮结束原因。设计取舍见 [`docs/design.md`](docs/design.md)。

## 运行方式

```text
make test     # 52 个测试：身份/指纹/合并、四协议、回退、续采、确认丢失、覆盖范围
make run      # 兼容旧流程：离线摄取 fixtures/input.json（幂等 upsert）
make clean    # 清理缓存与本地数据库
```

CLI 子命令（`PYTHONPATH=src python3 -m scrollharvest.cli ...`）：

```text
ingest   兼容旧流程：读 fixtures/input.json 落 scrollharvest.db（默认）
harvest  动态采集演示：可变异内存源跑完整引擎，落 demo_harvest.db
query [identity]  查询有效记录、覆盖范围、运行台账，以及某身份的修订链
```

## 数据流与模块边界

引擎只做编排与事务边界，业务判断各自单一职责：

| 模块 | 职责 |
|---|---|
| `sources` | `ListSource` 抽象 + 离线可变异内存源（代际/TTL 失效、稳定快照、显式删除事件） |
| `pagination` | 四协议分页器与 `PaginationPlan` 回退顺序 |
| `keyer` | 版本化稳定身份键（URL 规范化、字段顺序无关） |
| `fingerprint` | 版本化内容指纹（fp.v1/fp.v2）与业务内容指纹（相似记录检测） |
| `merge` | 增量合并决策：insert/update/unchanged/delete/similar |
| `cursor_state` | 检查点状态机与覆盖窗口三态（covered/pending/unconfirmable） |
| `convergence` | 预算（页/时长/字节/空页）与停止条件 |
| `engine` | 完整采集循环：inflight 落盘 → 取页 → 单事务确认 → 推进检查点 |
| `store` | SQLite 七张表：records/revisions/write_ledger/tombstones/harvest_state/coverage/runs |
| `loader / normalizer / cli / pipeline` | 兼容入口、规范化原语与编排 |

## 关键正确性约定

1. **确认后才推进**：请求先持久化为 inflight；整页单事务确认入库后才推进
   恢复位置。崩溃重放 inflight，写台账保证不重复写入。
2. **确认丢失安全**：`commit()` 确认丢失时不推进检查点，重放由台账去重，
   同一身份的生效写入只出现一次。
3. **游标失效可续**：旧协议窗口降级 pending，按计划换协议从首页重走，
   幂等层短路已确认身份。
4. **无快照不宣称完整**：连续空页只触发静止轮询；结尾标
   `unconfirmable`，以 `end_reached_unverified` 结束，pending 由 repair 补采。
5. **规则版本可解释**：身份键与内容指纹规则只增不改，历史修订携带规则号，
   换版后仍能解释历史结果。

## 持久化

所有状态在项目内 SQLite：稳定键幂等写入、修订链留痕、删除墓碑、覆盖范围
与运行台账。测试使用临时库，不污染工作区；`*.db` 已在 `.gitignore` 中。
