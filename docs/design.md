# 采集可靠性设计说明

面向“源站在采集期间持续插入、编辑、删除”的动态列表，说明四协议分页、增量
合并、断点续采的完整流程，以及回退范围、停止条件、状态保留三个关键取舍。

## 1. 总体流程与模块边界

```text
loader/normalizer（规整、规范化原语）
        │
sources.ListSource ── fetch(PageRequest) ── Page(items, next, deletions)
        ▲
pagination（四协议分页器 + PaginationPlan 回退顺序）
        │
engine.HarvestEngine（编排 + 事务边界 + 检查点 + 收敛）
   ├── merge.classify_record      新增/更新/未变/删除/相似 决策
   ├── keyer.stable_key           版本化稳定身份键
   ├── fingerprint                版本化内容指纹 + 业务内容指纹
   ├── cursor_state               检查点状态机 + 覆盖窗口
   └── convergence                预算与停止条件
        ▼
store（sqlite：records / revisions / write_ledger /
        tombstones / harvest_state / coverage / runs）
```

引擎只做编排与事务边界；每个业务判断都在单一职责模块内，可独立测试。

## 2. 四种分页协议

| 协议 | 恢复位置 | 动态数据下的弱点 | 适用 |
|---|---|---|---|
| offset | `(offset, limit)` | 头部插入/删除导致整窗漂移，重复或漏行 | 源只支持 offset 时兜底 |
| cursor | 服务端不透明游标（含代际+边界） | 代际变化/TTL 后失效 | 主流列表 API |
| scroll（无限滚动） | 首屏无上下文，之后靠 `scroll_id` | 同 cursor，且上下文通常更短命 | 无限滚动接口 |
| time_anchor | 时间锚点向历史翻页，可带 `floor_anchor` 限定范围 | 早于锚点的回填记录需要补采 | 有可靠 `updated_at` 的源 |

默认回退顺序 `time_anchor → cursor → scroll → offset`：越靠前的协议对动态
数据越健壮，offset 放最后并必须配合覆盖窗口重走收敛。协议切换时**从新协议
首页重走**，已确认身份由写台账幂等短路，不产生重复写入。

## 3. 身份、内容与合并规则

### 3.1 稳定身份键（`keyer.key.v1`，只增不改）

优先级：源站 `id` → 规范化 URL（剔除 `utm_*` 等跟踪参数、去 fragment、
大小写归一、查询串排序）→ 业务字段兜底哈希（`sort_keys` 规范化 JSON，
字段顺序无关）。因此：

- URL 跟踪参数/大小写/fragment 变化、字段顺序变化 **不会产生新记录**；
- 两条业务内容相同但身份不同的记录是**相似独立记录**，标 `similar` 并各自
  保留，绝不按相似度合并。

### 3.2 版本化内容指纹（`fp.v1` / `fp.v2`）

- `fp.v1`：剔除抓取期易变键（`_fetch_at` 等）后哈希业务内容；
- `fp.v2`：再剔除服务端触碰时间（`ts/updated_at/fetched_at`），仅重打
  时间戳不构成内容变更。

规则版本号随每次修订入库。换版时不静默改写历史：合并模块把新旧记录放到
**同一目标规则**下重算规范化内容体，内容一致判 `unchanged` 并在修订原因中
记录 `rule migration ...`；历史修订永久保留产生它的规则号，可随时解释。

### 3.3 合并动作

`insert / update / unchanged / delete / similar`。删除**只**由源站显式
删除事件（`Deletion`）触发——记录在某页“缺席”永远不推断为删除。删除写
墓碑并把活动记录标 `is_deleted=1`；同 id 之后合法复活时，作为墓碑之上的
新 `insert` 修订，删除历史仍可追溯。

## 4. 断点续采与“确认后才推进”

### 4.1 检查点状态机（`cursor_state.CheckpointPhase`）

`new → inflight → confirmed → done`，异常分支 `cursor_dead`、补采分支
`repair`。铁律：

1. **请求先落盘为 inflight，再发往源站**；
2. 整页记录在**单事务**内完成 `台账检查 → 写记录/修订/墓碑 → 覆盖窗口`，
   事务提交成功后才把恢复位置推进到下一页；
3. 进程在任何时刻崩溃：inflight 请求仍在 `harvest_state` 中，重启重放该
   请求，由 `write_ledger` 去重。

### 4.2 提交确认丢失（ConfirmationLost）

`commit()` 可能在事务实际落库后、确认返回前失败，此时状态不可知。引擎
**不吞异常、不推进检查点**，保留 inflight 并以 `fatal` 结束本轮；重跑时：

- 若事务已落库：台账命中，跳过写入；
- 若事务未落库：正常写入。

两条路径都保证同一身份的生效写入（insert/update/delete）只出现一次。
幂等键 = `run_id | protocol:page_index | identity`，同一 run 的重放与
跨 run 的重采各自有确定性去重依据。

### 4.3 游标失效与协议切换

游标携带源结构代际与签发时间。源站 insert/update/delete 提升代际，或 TTL
超时，旧游标抛 `CursorExpired`：该协议下已 `covered` 的窗口**降级为
pending**（记录本身不丢，但源站侧覆盖在新代际下不再可证），引擎按
`PaginationPlan` 切换下一协议并从首页重走。所有协议都不支持时以
`protocol_exhausted` 失败，等待源恢复后续采，不伪造成功。

## 5. 覆盖范围三态与“不能宣告完整”

`coverage` 表对每个窗口记录三态：

| 状态 | 含义 |
|---|---|
| covered | 已确认遍历并落库（有 offset 索引/游标/锚点依据） |
| pending | 待补：边界漂移、中断、游标失效后未在新代际下重新确认 |
| unconfirmable | 无法确认：无稳定快照时，源站结尾之外是否存在迟到/回填插入不可证明 |

**没有稳定源快照时，连续若干空页绝不等于完整采集**：引擎只做有限次数的
静止轮询（回首页重查，幂等短路），轮询期发现新变化会重置计数继续采集；
轮询用尽后以 `end_reached_unverified` 结束，尾部窗口标 `unconfirmable`，
并在 `runs.detail` 中说明原因。pending 窗口通过后续 `repair` 补采收敛；
unconfirmable 是诚实的“未知”，不会被补采强行改写成 covered。

## 6. 停止条件（`convergence.EndReason`）

| 结束原因 | 状态 | 含义 |
|---|---|---|
| complete | ok | 稳定快照固定且源站明确给完结尾 |
| end_reached_unverified | ok* | 无快照走到结尾，结果可用但不可声称完整 |
| quiesced | ok* | 静止轮询通过，pending 保留 |
| budget_pages / budget_runtime / budget_bytes / budget_empty_streak | resumable | 预算暂停，检查点保留，续采接着跑 |
| protocol_exhausted | failed | 所有协议不可用 |
| fatal | failed | 崩溃/确认丢失，inflight 保留可重放 |

\* `ok` 表示“本轮正常结束、数据可查询使用”，但 `can_claim_complete=False`。

预算是**可恢复的暂停而非失败**：页数、运行时长、累计字节、连续空页四类
预算在每次发请求前检查；`HarvestBudget.enforce` 另提供异常式出口。

## 7. 三个关键取舍

### 7.1 回退范围（rollback scope）

- **页内**：单事务（SAVEPOINT + commit），任何一条记录失败 → 整页回滚，
  恢复位置不推进，重放整页；
- **协议级**：游标失效只回退该协议未确认的窗口（pending），已入库记录不
  回滚，新协议从首页重走由幂等层收敛；
- **规则级**：指纹/身份规则换版不回滚历史数据，新旧版本并存，迁移结果在
  修订原因中标注；
- **运行级**：整轮失败不删除已确认数据，`runs` 状态标 failed/resumable，
  下一轮从检查点继续。代价是库里会同时存在多轮运行的台账与修订历史，
  靠保留策略（见 7.3）控制体积。

### 7.2 停止条件的取舍

- 选择**预算 + 静止轮询**而不是“空页即停”：前者在动态源下安全（宁可多
  轮询也不漏），代价是多取若干重复页（由台账短路，成本仅请求与比对）；
- 无快照源永远给不出 `complete`，只给 `end_reached_unverified`：把“未知”
  暴露给下游，而不是用确定性语气掩盖不可证明的范围；
- `quiet_polls=0` 可显式关闭轮询（适合调度器周期性补采的场景），此时
  单轮结论仍是 unverified。

### 7.3 状态保留的取舍

| 状态 | 保留理由 | 代价/控制 |
|---|---|---|
| write_ledger | 确认丢失重放、跨 run 去重的唯一依据 | 只增；可按 run_id 归档旧 run 后清理，最近一个未完成 run 的台账必须保留 |
| revisions | 回答“这条记录为什么是现在的值、由哪轮采集写入” | 每身份一条修订链；合规要求长留，空间敏感时只压缩 unchanged 修订 |
| tombstones | 删除可追溯、区分“没采到”与“被删除” | 小表，建议长期保留 |
| harvest_state | 断点续采恢复位置 | 每 source/stream 单行 upsert，体积恒定 |
| coverage | 已覆盖/待补/无法确认的范围声明 | 窗口数与页数同级，repair 收敛后可归档 |
| runs | 回答“本轮为什么结束” | 每轮一行，建议按保留期滚动删除 |

## 8. 有限资源下的大页与持续变化数据

- 页大小有硬上限 `max_page_size`（默认 5000），超限必须显式放宽，防止
  单次拉取失控；字节预算独立于页数预算，防止“小页数、大单页”绕开限制；
- 引擎逐页处理、逐页提交，不缓存全量列表，内存占用与单页大小同阶；
- 持续变化的源靠“预算暂停 → 检查点续采 → repair 收敛 pending → 周期性
  补采 unconfirmable 尾部”闭环，而不是指望单轮追上所有变化；
- 静止轮询与重放请求在统计中单列 `poll_fetches` / `ledger_dedup`，
  可区分“真实分页推进”与“幂等重取”。

## 9. 最终可查询的三类答案

1. **有效记录**：`store.valid_records(con)`（`is_deleted=0`）；
2. **修订来源**：`store.revisions_of(con, identity)`，含每次动作、指纹
   规则版本、来源、run_id、原因与时间；
3. **本轮结束原因**：`store.run_history(...)` 与每轮返回的
   `end_reason / can_claim_complete / detail / coverage / stats`。

## 10. 兼容性

旧的 fixtures 离线流水线（`pipeline.run` / `cli ingest`）与既有输入
`fixtures/input.json` 保持可用，落库表新增列均为可空/带默认值，旧库
打开时自动补列（`ALTER TABLE ... ADD COLUMN` 容错）。
