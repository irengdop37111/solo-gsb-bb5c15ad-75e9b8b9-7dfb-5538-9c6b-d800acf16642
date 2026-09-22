# 设备遥测接收 API（device-telemetry-ingest）

基于 **FastAPI + SQLite** 的本地设备遥测接收服务。设备按批次上报带递增序列号的遥测记录，批次可能乱序到达；服务为每台设备维护一个**滑动窗口**，实现乱序暂存、重复幂等丢弃、超时断档标记与断档重放，并在并发请求下保持状态一致。另支持**连续阈值告警**：按设备配置规则，对已提交记录按序检测连续越限并生成可查询告警；每条告警携带 `open`/`acknowledged` 状态，可通过**幂等确认接口**由确认人标记为已确认（记录确认人与确认时间，重复确认不改变首次确认信息）。**告警规则版本化**：修改规则不会原地改动旧配置，而是追加一个递增版本并可指定生效序列号（`effective_from_seq`）；每条 committed 记录在写事务内按序列号**原子选择唯一生效版本**评估，历史版本、每版本连续计数状态与已生成告警均永久保留，另提供版本列表与按版本筛选告警接口。

## 快速启动

### Docker Compose（推荐）

```bash
docker compose up --build
```

### 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 启动后

| 项目 | 地址 |
|---|---|
| API 基础地址 | `http://localhost:8000/api/v1` |
| 交互式文档 (Swagger) | `http://localhost:8000/docs` |
| 健康检查 | `http://localhost:8000/health` |
| 服务信息/当前配置 | `http://localhost:8000/` |
| 端口 | `8000`（Compose 映射 `8000:8000`，可在 `docker-compose.yml` 修改） |

SQLite 数据库文件默认在容器内 `/data/telemetry.db`，Compose 通过命名卷 `telemetry-data` 持久化。升级镜像时启动初始化会自动迁移旧库，无需手动干预（但仍建议升级前备份卷内数据库）：为旧库 `alerts` 表补齐 `status`/`acknowledged_by`/`acknowledged_at` 列（历史告警视为 `open`）；升级到带**规则版本化**的镜像时，会把旧 `alert_rules` 表重建为“规则身份 + 不可变版本”结构，存量规则导入为**版本 1、生效序号 0**，原连续计数状态迁移到版本 1 的独立状态行，历史告警标记为 `rule_version = 1`，自增主键计数也会保留。迁移均为幂等，可重复启动。

## 配置项（环境变量）

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `TELEMETRY_DB_PATH` | `/data/telemetry.db` | SQLite 数据库文件路径 |
| `TELEMETRY_WINDOW_SIZE` | `1000` | 每设备滑动窗口大小：接受 `[next_expected, next_expected + window_size)` 内的序列号 |
| `TELEMETRY_GAP_TIMEOUT_SECONDS` | `30` | 缺口等待时间（秒），超时标记为断档 |
| `TELEMETRY_SWEEP_INTERVAL_SECONDS` | `5` | 后台扫描间隔（秒），无流量时也会判定断档 |
| `TELEMETRY_SWEEP_ENABLED` | `true` | 是否启用后台扫描（测试时可关闭，断档判定退化为访问时惰性触发） |
| `TELEMETRY_CURSOR_SECRET` | `telemetry-cursor-dev-secret` | 分页游标 HMAC 签名密钥；生产环境务必改为强随机值（`docker-compose.yml` 支持同名 shell 变量覆盖） |

## 核心语义

每台设备维护 `next_expected`（下一个期望的连续序列号），接收窗口为
`[next_expected, next_expected + window_size)`。设备首次出现时，以该批最小序列号锚定窗口起点。

| 到达序列号位置 | 处理 | 返回状态 |
|---|---|---|
| `seq == next_expected` | 落库为已提交，窗口前移，并连续冲刷已暂存的记录 | `committed` |
| 窗口内、`next_expected` 之后 | 暂存，等待前方缺口补齐 | `buffered` |
| 已存在相同序列号 | 幂等丢弃，不重复落库（批次整体重试安全） | `duplicate` |
| 窗口之下、属于已声明断档 | 作为补录数据填充断档 | `gap_filled` |
| 窗口之下、无记录、无断档、距窗口 ≤ window_size 且从未断档 | 窗口回锚（并发首批竞争导致锚点偏高时向下修正）后正常接收 | `committed`/`buffered` |
| 窗口之下、其余情况 | 过旧数据，忽略 | `stale` |
| 超出窗口上界 | 拒绝，客户端应在窗口前移后重试 | `out_of_window` |

**断档（gap）**：`next_expected` 与最小暂存序列号之间存在缺口，且持续超过
`TELEMETRY_GAP_TIMEOUT_SECONDS`，即标记断档：写入 `gaps` 表（状态 `open`），
`next_expected` 跳过缺口，暂存记录继续冲刷。断档由后台扫描器周期性判定，也会在每次请求时惰性判定。

**断档重放**：客户端查询断档 → 调用重放接口（状态转 `replaying`，返回缺失序列号区间）
→ 设备通过正常批次接口重发缺失序列号 → 逐条落为 `gap_filled` → 缺口全部补齐后断档自动转为 `recovered`。

**连续阈值告警**：每台设备可配置多条告警规则（payload 顶层数值字段 + `gt`/`lt` + 阈值 +
连续条数 N）。规则只在记录**首次转为 `committed`** 时按 seq 顺序推进——直接提交与一次提交
冲刷多条 `buffered` 记录都会逐条计算；`duplicate`/`stale`/`out_of_window`/`gap_filled`
不参与计数，因此重复批次不会重复计数，断档补录也不回溯重算。遇到**声明断档**或**字段缺失/
非数值**时连续计数中断（streak 清零并重新待命）。连续越限首次达到 N 条时生成一条告警，
同一连续区段不再重复生成；待计数中断（恢复正常）后，下一段连续越限才会再次触发。
规则创建前已提交的历史记录不回溯计算。

**告警规则版本化切换**：规则修改不会原地更新配置，而是为该规则追加一条**不可变版本**
（版本号在规则内从 1 开始单调递增）。每个版本携带 field/op/threshold/consecutive 的完整快照
和一个**生效序列号** `effective_from_seq`：记录 seq 首次转为 `committed` 时，在同一个
`BEGIN IMMEDIATE` 写事务内按

```
SELECT ... FROM alert_rule_versions
WHERE rule_id = :id AND effective_from_seq <= :seq
ORDER BY effective_from_seq DESC, version DESC LIMIT 1
```

**原子选择该 seq 的唯一生效版本**（相同生效序号时版本号更高者胜出，可表达“立即替换”），
再用**该版本独立的连续计数状态**评估。因此无论规则修改与批次提交/buffered 冲刷如何并发，
每条记录序列号只会被一个版本评估，不重不漏：

- `effective_from_seq` 可在修改时显式指定；省略时默认取设备当前 `next_expected`（即“从此刻起”）。
- 生效序号只允许**单调不回退**（不得小于现有最新版本的生效序号），否则返回 `422`；
  允许与最新版本相同，表示从该序号起以新版本为准（同序号新版本号决胜）。
- 切换出的新版本从**冷状态**（streak=0）开始；旧版本的连续计数既不被继承也不被清零。
- 历史版本、各版本状态以及已经生成的告警（连同 field/op/threshold/consecutive 快照与
  `rule_version`）**永不改写**；即使之后再次修改规则，旧告警仍指向产生它的版本。
- 生效序号高于当前记录的版本不评估这些记录（可“预置”未来生效的配置）；规则创建前的
  历史记录同样不回溯。
- 声明断档会重置该设备所有规则所有版本的连续计数。

**告警确认工作流**：每条告警生成时状态为 `open`，并带 `acknowledged_by`/`acknowledged_at`
两个空字段。运维方可按告警 ID 调用确认接口，将其置为 `acknowledged` 并记录**确认人标识**
与**服务端确认时间**。确认接口**幂等**：重复确认（无论是否换人）都返回当前记录，
首次确认人与确认时间**永不改变**。

**并发一致性**：每请求一个 SQLite 连接（WAL 模式 + `busy_timeout`），所有状态变更包裹在
`BEGIN IMMEDIATE` 写事务中，写操作在数据库层面串行化；同步端点运行在线程池中，
配合每序列号幂等，并发/重试/乱序下状态保持一致。告警状态与产生它的记录状态变更
在同一事务内提交。**规则生效版本的选择、每版本状态推进与告警插入同样在该记录提交
（直接提交或 buffered→committed 冲刷）的同一事务内完成**，因此规则修改与批次并发时，
按记录序号选择生效版本是原子且唯一的。告警确认使用“仅当 `status = 'open'` 才更新”的
条件 UPDATE，并发确认在数据库层串行化，恰好一个请求完成首次确认，其余请求读到一致的
已确认记录。

## API 一览

### 提交批次

```
POST /api/v1/batches
```

```json
{
  "device_id": "dev-001",
  "batch_id": "optional-client-id",
  "records": [
    {"seq": 1, "payload": {"temp": 21.5}},
    {"seq": 2, "payload": {"temp": 21.6}}
  ]
}
```

响应（每条记录独立返回处理结果，HTTP 恒为 200，除非请求体校验失败）：

```json
{
  "device_id": "dev-001",
  "batch_id": "optional-client-id",
  "accepted": 2,
  "results": [{"seq": 1, "status": "committed"}, {"seq": 2, "status": "buffered"}],
  "window": {"next_expected": 2, "buffered_count": 1}
}
```

### 查询窗口状态

```
GET /api/v1/devices/{device_id}/window
```

返回 `next_expected`、窗口范围、暂存序列号列表、待判定缺口（含剩余等待时间）、
已声明断档列表与统计计数（committed/buffered/gap_filled/duplicates）。

### 一致性分页查询记录

```
GET /api/v1/devices/{device_id}/records?seq_from=1&seq_to=100&limit=50
GET /api/v1/devices/{device_id}/records?cursor=<next_cursor>
```

按 `seq` 升序做**键集分页**（keyset pagination），参数：

| 参数 | 说明 |
|---|---|
| `seq_from` / `seq_to` | 可选，序列号闭区间过滤（仅首页请求需要传入） |
| `limit` | 可选，每页条数，默认 100，最大 1000 |
| `cursor` | 可选，上一页响应返回的 `next_cursor`；传入后区间与页大小以游标为准 |

**一致性语义**：首次请求固定一个 SQLite rowid 高水位（snapshot 边界），并随不透明
`next_cursor` 一起返回；后续页沿用该高水位与查询区间。翻页期间到达的新批次、断档补录
（`gap_filled`）的 rowid 都大于高水位，因此**不会混入**本次记录集合；已存在的记录原地
更新（如 `buffered` → `committed`）不改变 rowid，**不会漏读**；键集翻页保证每条记录
**恰好返回一次**。集合成员固定，但记录字段（如 `status`）反映读取时的最新值。

**游标安全**：游标为 base64url(JSON) + HMAC-SHA256 签名，密钥取自
`TELEMETRY_CURSOR_SECRET`。以下情况均返回 **400**：签名不符或格式非法（篡改）、
跨设备复用、复用时更改 `seq_from`/`seq_to`/`limit`。与游标一致的重复参数允许传入。
未知设备返回 404；`seq_from > seq_to` 返回 400。

响应示例（无下一页时 `next_cursor` 为 `null`）：

```json
{
  "device_id": "dev-001",
  "records": [
    {"seq": 1, "payload": {"temp": 21.5}, "status": "committed", "received_at": 1758000000.12}
  ],
  "next_cursor": "eyJ2IjoxLC...9oZg.4f3b2a...9c"
}
```

### 查询断档

```
GET /api/v1/devices/{device_id}/gaps?status=open
```

`status` 可选过滤：`open` / `replaying` / `recovered`。

### 断档重放

```
POST /api/v1/devices/{device_id}/gaps/{gap_id}/replay
```

将断档标记为 `replaying` 并返回缺失区间，例如：

```json
{
  "gap": {"gap_id": 1, "start_seq": 4, "end_seq": 6, "status": "replaying", "...": "..."},
  "missing_ranges": [[4, 6]],
  "missing_count": 3
}
```

设备随后经 `POST /api/v1/batches` 重发缺失序列号即可补齐。

### 创建告警规则

```
POST /api/v1/devices/{device_id}/alert-rules
```

```json
{
  "field": "temp",
  "op": "gt",
  "threshold": 30.0,
  "consecutive": 3
}
```

`field` 为 payload 顶层数值字段名；`op` 取 `gt`（大于阈值越限）或 `lt`（小于阈值越限）；
`consecutive` 为触发告警所需的连续越限条数 N（≥ 1）。设备不存在也可先建规则
（设备由首个批次惰性创建），规则只评估创建之后新提交的记录。返回 `201`，新规则为
**版本 1、生效序号 0**：

```json
{
  "rule_id": 1,
  "device_id": "dev-001",
  "field": "temp",
  "op": "gt",
  "threshold": 30.0,
  "consecutive": 3,
  "created_at": 1758000000.12,
  "version": 1,
  "effective_from_seq": 0,
  "state": {"last_seq": null, "streak": 0, "alerted": false},
  "versions": [
    {"rule_id": 1, "version": 1, "field": "temp", "op": "gt",
     "threshold": 30.0, "consecutive": 3, "effective_from_seq": 0,
     "created_at": 1758000000.12,
     "state": {"last_seq": null, "streak": 0, "alerted": false}}
  ]
}
```

规则级字段（field/op/threshold/consecutive/state）始终镜像**最新版本**，`versions` 为
该规则的全部历史版本（含每版本独立状态）。

### 修改告警规则（追加新版本）

```
PUT /api/v1/devices/{device_id}/alert-rules/{rule_id}
```

请求体与创建规则一致，另含可选的 `effective_from_seq`（≥ 0）：

```json
{
  "field": "temp",
  "op": "gt",
  "threshold": 50.0,
  "consecutive": 2,
  "effective_from_seq": 1000
}
```

- 成功返回 `200`，结构与规则列表项相同：版本号 +1，新版本从此前从未评估过的冷状态开始。
- `effective_from_seq` 省略时默认取设备当前 `next_expected`（设备尚不存在时为 0）。
- 生效序号不得小于当前最新版本的生效序号；回退返回 `422`（错误信息给出当前下界）。
  允许与最新版本**相同**（从该序号起以新版本为准）。
- 规则 ID 不属于该设备（含不存在）返回 `404`；参数校验失败返回 `422`。
- 修改是**追加**：旧版本、旧版本状态与已生成告警均保持不变。

### 告警规则版本列表

```
GET /api/v1/devices/{device_id}/alert-rules/{rule_id}/versions
```

按版本号升序返回该规则的全部不可变版本，每个版本带配置快照、`effective_from_seq`、
创建时间与该版本独立的实时状态：

```json
{
  "device_id": "dev-001",
  "rule_id": 1,
  "versions": [
    {"rule_id": 1, "version": 1, "field": "temp", "op": "gt",
     "threshold": 30.0, "consecutive": 3, "effective_from_seq": 0,
     "created_at": 1758000000.12,
     "state": {"last_seq": 999, "streak": 999, "alerted": true}},
    {"rule_id": 1, "version": 2, "field": "temp", "op": "gt",
     "threshold": 50.0, "consecutive": 2, "effective_from_seq": 1000,
     "created_at": 1758000100.00,
     "state": {"last_seq": null, "streak": 0, "alerted": false}}
  ]
}
```

规则不属于该设备返回 `404`。

### 告警规则列表

```
GET /api/v1/devices/{device_id}/alert-rules
```

返回该设备全部规则；规则级字段为最新版本快照及其实时状态，`versions` 内嵌全部历史版本：
各版本的 `streak`（当前连续越限条数）、`alerted`（当前连续区段是否已触发告警）、
`last_seq`（该版本最近参与计算的 committed 序列号）。

### 查询告警

```
GET /api/v1/devices/{device_id}/alerts
GET /api/v1/devices/{device_id}/alerts?rule_id=1
GET /api/v1/devices/{device_id}/alerts?rule_version=2
GET /api/v1/devices/{device_id}/alerts?status=open
GET /api/v1/devices/{device_id}/alerts?rule_id=1&rule_version=2&status=acknowledged
```

`rule_id`、`rule_version` 与 `status` 均为可选过滤，可任意组合：

| 参数 | 说明 |
|---|---|
| `rule_id` | 仅返回该规则（任意版本）产生的告警 |
| `rule_version` | 仅返回由规则的**该版本**产生的告警（版本切换后便于按版本追溯） |
| `status` | `open`（未确认）/ `acknowledged`（已确认）；不传则保留**全部历史** |

每条告警记录越限区段的 `start_seq`/`end_seq`（end 为达到连续条数
的那条记录）、触发值 `trigger_value`、产生它的 `rule_version`、规则快照
（field/op/threshold/consecutive）以及确认工作流字段
（`status`、`acknowledged_by`、`acknowledged_at`）。
过滤只筛选行，响应始终保留告警的全部字段：

```json
{
  "device_id": "dev-001",
  "alerts": [
    {
      "alert_id": 1,
      "rule_id": 1,
      "rule_version": 1,
      "device_id": "dev-001",
      "field": "temp",
      "op": "gt",
      "threshold": 30.0,
      "consecutive": 3,
      "start_seq": 1,
      "end_seq": 3,
      "trigger_value": 33.0,
      "created_at": 1758000000.34,
      "status": "open",
      "acknowledged_by": null,
      "acknowledged_at": null
    }
  ]
}
```

### 确认告警

```
POST /api/v1/devices/{device_id}/alerts/{alert_id}/acknowledge
```

请求体（`acknowledged_by` 为必填的确认人标识，1–128 字符）：

```json
{"acknowledged_by": "ops-alice"}
```

- 首次确认：告警 `open` → `acknowledged`，记录确认人与服务端确认时间，返回 `200` 与完整告警。
- **重复确认幂等**：对同一告警再次调用（即使换成另一个确认人）仍返回 `200` 与当前记录，
  首次确认的 `acknowledged_by`/`acknowledged_at` 不会被修改。
- **并发安全**：多个确认请求同时到达时，`BEGIN IMMEDIATE` 事务与条件更新保证恰好一次
  首次确认生效，所有响应的确认人/确认时间一致。
- 告警 ID 不属于该设备（含不存在）返回 `404`；请求体校验失败返回 `422`。

确认后响应示例：

```json
{
  "alert_id": 1,
  "rule_id": 1,
  "device_id": "dev-001",
  "field": "temp",
  "op": "gt",
  "threshold": 30.0,
  "consecutive": 3,
  "start_seq": 1,
  "end_seq": 3,
  "trigger_value": 33.0,
  "created_at": 1758000000.34,
  "status": "acknowledged",
  "acknowledged_by": "ops-alice",
  "acknowledged_at": 1758000012.80
}
```

### 其他

- `GET /api/v1/devices` — 设备列表
- `GET /health` — 健康检查
- `GET /` — 服务信息与当前配置

## 完整流程示例

```bash
# 1. 乱序提交：1,2,3,5,6（缺口 4）
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id": "dev-001",
  "records": [{"seq":1,"payload":{}},{"seq":2,"payload":{}},{"seq":3,"payload":{}},
              {"seq":5,"payload":{}},{"seq":6,"payload":{}}]
}'
# -> 1,2,3 committed；5,6 buffered

# 2. 查询窗口：pending_gap 显示缺口 4 及剩余等待时间
curl localhost:8000/api/v1/devices/dev-001/window

# 3. 超过 GAP_TIMEOUT 后，断档被标记（后台扫描或下次访问时）
curl localhost:8000/api/v1/devices/dev-001/gaps
# -> gap_id=1, start=4, end=4, status=open

# 4. 请求重放
curl -X POST localhost:8000/api/v1/devices/dev-001/gaps/1/replay
# -> status=replaying, missing_ranges=[[4,4]]

# 5. 设备重发 seq 4
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id": "dev-001", "records": [{"seq":4,"payload":{"temp":21.7}}]
}'
# -> status=gap_filled；断档自动 recovered

# 6. 一致性分页拉取记录（第一页，固定高水位快照）
curl 'localhost:8000/api/v1/devices/dev-001/records?seq_from=1&seq_to=100&limit=2'
# -> {"records": [...seq 1, 2...], "next_cursor": "eyJ2Ijox..."}

# 7. 后续页只带 cursor；期间新到的批次/补录不会混入本次结果
curl 'localhost:8000/api/v1/devices/dev-001/records?cursor=eyJ2Ijox...'
# -> 翻页直至 next_cursor 为 null，区间内每条记录恰好返回一次

# 8. 创建连续阈值告警规则：temp 连续 3 条 > 30 触发
curl -X POST localhost:8000/api/v1/devices/dev-001/alert-rules \
  -H 'Content-Type: application/json' \
  -d '{"field":"temp","op":"gt","threshold":30,"consecutive":3}'
# -> 201 {"rule_id": 1, ..., "state": {"streak": 0, "alerted": false}}

# 9. 上报连续越限数据；达到 3 条时生成一条告警
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id": "dev-001",
  "records": [{"seq":7,"payload":{"temp":31}},{"seq":8,"payload":{"temp":32}},
              {"seq":9,"payload":{"temp":33}}]
}'
curl localhost:8000/api/v1/devices/dev-001/alerts
# -> alerts[0]: start_seq=7, end_seq=9, trigger_value=33.0, status=open
#    同一连续区段不再重复告警；恢复正常（未越限/字段缺失/声明断档）后才可再次触发

# 10. 确认告警：记录确认人与确认时间；重复/并发确认幂等，首次确认信息不变
curl -X POST localhost:8000/api/v1/devices/dev-001/alerts/1/acknowledge \
  -H 'Content-Type: application/json' \
  -d '{"acknowledged_by":"ops-alice"}'
# -> 200 {"alert_id": 1, "rule_version": 1, "status": "acknowledged",
#         "acknowledged_by": "ops-alice", "acknowledged_at": 1758000012.8, ...}
# 仅看待确认 / 已确认告警：
curl 'localhost:8000/api/v1/devices/dev-001/alerts?status=open'
curl 'localhost:8000/api/v1/devices/dev-001/alerts?status=acknowledged'

# 11. 查看规则实时状态（最新版本 + 内嵌历史版本）
curl localhost:8000/api/v1/devices/dev-001/alert-rules

# 12. 修改规则：追加版本 2，自 seq 1000 起改为 temp>50 连续 2 条
curl -X PUT localhost:8000/api/v1/devices/dev-001/alert-rules/1 \
  -H 'Content-Type: application/json' \
  -d '{"field":"temp","op":"gt","threshold":50,"consecutive":2,"effective_from_seq":1000}'
# -> 200 {"rule_id": 1, "version": 2, "effective_from_seq": 1000,
#         "state": {"last_seq": null, "streak": 0, "alerted": false},
#         "versions": [ {版本 1 ...}, {版本 2 ...} ]}
# 省略 effective_from_seq 时默认取当前 next_expected（从此刻起生效）
# 生效序号回退（< 现有最新值）返回 422

# 13. 查看全部版本及其独立状态；按版本筛选历史告警
curl localhost:8000/api/v1/devices/dev-001/alert-rules/1/versions
curl 'localhost:8000/api/v1/devices/dev-001/alerts?rule_version=1'
curl 'localhost:8000/api/v1/devices/dev-001/alerts?rule_version=2'
# 之后提交 seq >= 1000 的记录将由版本 2 评估；版本 1 与其产生的告警不再变化
```

## 项目结构

```
app/
  config.py   # 环境变量配置
  db.py       # SQLite 连接、建表、写事务（WAL + BEGIN IMMEDIATE）
  window.py   # 滑动窗口核心引擎（提交/暂存/断档/重放/回锚）
  alerts.py   # 连续阈值告警引擎（版本化规则/按 seq 选版本/断档中断/告警生成）
  cursor.py   # HMAC 签名的不透明分页游标（高水位 + 键集位置）
  models.py   # 请求/响应模型
  main.py     # FastAPI 应用、路由、后台断档扫描器
tests/
  test_api.py          # 乱序/幂等/断档/重放/回锚/并发一致性测试
  test_records.py      # 一致性分页：快照隔离/游标篡改/跨设备/区间变更测试
  test_alerts.py       # 连续阈值告警：规则 CRUD/连续计数/断档中断/补录不重算测试
  test_rule_versions.py # 规则版本化：版本追加/按 seq 生效/SQLite 迁移/并发切换测试
Dockerfile
docker-compose.yml
requirements.txt / requirements-dev.txt
```

## 运行测试

```bash
pip install -r requirements-dev.txt
pytest tests/ -q
```

测试覆盖：顺序提交、乱序暂存与冲刷、重复幂等（含批内重复）、窗口外拒绝、
断档超时标记、重放恢复（含多序列号部分补齐）、窗口回锚、设备自举，
8 线程并发提交 200 条记录后的状态一致性校验（真实 uvicorn 服务），
以及一致性分页：多页不重不漏、翻页期间新批次/断档补录不混入、原地状态变更不漏读、
游标篡改/跨设备/更改区间复用均返回 400。
连续阈值告警：规则创建/校验/列表、gt/lt 越限判定、首次达 N 条生成告警且同区段不重复、
恢复正常后再次触发、字段缺失/非数值中断计数、buffered 冲刷逐条按序计算、
重复批次不重复计数、stale/out_of_window 不参与、声明断档中断连续计数、
断档补录不回溯重算、规则创建前历史不回溯、多规则独立评估。
告警确认工作流：新告警默认 open 且确认字段为空、首次确认记录确认人与时间且保留全部
历史字段、重复确认幂等且不改首次确认信息（换人也不可覆盖）、按状态筛选（可与 rule_id
组合，非法值 422）、未知/跨设备告警 404、请求体校验、旧版 SQLite 库启动自动迁移，
以及真实 uvicorn 服务下 16 并发确认单条告警时唯一确认人胜出、所有响应与落库状态一致。
规则版本化：版本 1 自 seq 0 起、修改追加递增版本且新版本冷启动、省略生效序号默认取
next_expected、生效序号回退 422（相同生效序号允许且新版本决胜）、按记录 seq 原子选择
版本（直接提交与 buffered 冲刷均逐条按序评估）、版本切换不继承/不清空旧版本计数、
历史版本与已生成告警不可变、版本列表与 `rule_version` 告警筛选（可与 rule_id/status
组合）、未知/跨设备规则 404、旧版 SQLite 库（含带旧状态表的库）启动自动迁移为版本
结构且幂等，以及真实 uvicorn 服务下：8 个同生效序号修改与单次大 buffered→committed
冲刷竞争时尾部记录唯一归属一个版本且计数空间不重不漏、8 个不同生效序号并发修改按
非回退规则串行成单调链且每条记录归属与最终版本表一致、两阶段并发批次与版本切换下
按 seq 边界 100/100 划分且告警不跨边界。
