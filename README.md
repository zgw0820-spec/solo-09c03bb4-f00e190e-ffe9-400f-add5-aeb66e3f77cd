# 巡检批量改派死锁预检服务

值班员在开放批量改派前，用本服务预先判断 2~3 批有序工单改派是否存在
**循环等锁（死锁）** 风险；发现风险时在本地测试库按检测到的交错顺序
实跑，以 PostgreSQL 返回 **SQLSTATE 40P01（deadlock_detected）**
作为复现成功的唯一判据。

## 1. 业务与锁模型

- 每一批是一笔 PostgreSQL 事务，按 `order_ids` 的**原顺序**逐单
  `UPDATE work_orders SET assignee=... WHERE order_id=...`；
- `UPDATE` 对命中行加排他行锁，**行锁持有到事务 COMMIT/ROLLBACK**；
- 两批共享工单时，后执行到该单的批次阻塞等待先持锁批次；
- 死锁的充要条件：存在一对工单 x、y 在两批中的加锁先后顺序反转
  （A 先 x 后 y、B 先 y 后 x），交错执行后 A 等 y、B 等 x 成环。

同号（同一工单在同一批内重复填写）在入参层直接拒绝：同一事务重复
更新同一行不会自锁，这类"同号并发试验"互不阻塞，对死锁判断无意义。

## 2. API

服务启动后访问 `http://localhost:8000/docs`（Swagger UI，内置三组
可一键填充的试跑样例）。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/orders` | 查看业务工单及负责人 |
| POST | `/orders/seed` | 幂等写入测试工单（负责人 `init`） |
| POST | `/precheck` | 死锁预检 + 可选实跑复现 |

### 请求约束

- 批次数 2~3；每批 1~6 单；
- 同一批内工单号不可重复；`batch_id` 不可重复；
- `run_replay=false` 时只做静态等待图分析，不连试跑通道。

### 响应要点（`/precheck`）

- `has_deadlock` / `cycles`：等待图上的全部有向环，环上标注
  `cycle_batches`（首尾相接的批次）与 `cycle_orders`（每条等待边
  争用的工单）；
- `wait_edges`：完整等待边，含等待方/持有方批次、工单号、双方在
  本批中的加锁步号；
- `replay.steps`：实跑全局操作序列，`acquire`（拿锁）/`block`
  （进入锁等待，含持有方 pid）/`abort`（被死锁检测中止，含 SQLSTATE）；
- `replay.batch_waits`：各批最终阻塞在哪张单、当前持有方是哪批、
  阻塞前已执行到第几步；
- `replay.cleanup_ok`：收尾核查（无未结束事务、试跑 schema 已销毁）。

## 3. 实跑复现原理

1. 在独立 schema `deadlock_trial` 建与业务表同构的临时表并写入工单；
2. 每批一个独立连接 + 线程，用 `threading.Barrier` 做全局锁步，
   保证环上各批按同一节拍交错，杜绝某批抢跑拿齐两把锁导致退化为
   普通等待；
3. 每个 `UPDATE` 由内部线程执行，工作线程轮询 `pg_stat_activity`
   的 `wait_event_type='Lock'`，真实记录 `block` 现场；行锁持有方
   通过行头 `xmax` 与 `pg_stat_activity.backend_xid` 关联得到；
4. PostgreSQL 死锁检测器（本环境 `deadlock_timeout=200ms`）中止
   牺牲者事务，捕获 `psycopg.errors.DeadlockDetected`，校验
   `sqlstate == '40P01'`；
5. 收尾：查 `pg_stat_activity` 确认无未结束事务 → `DROP SCHEMA
   deadlock_trial CASCADE` 物理销毁所有试跑数据与锁 → 复查 schema
   已不存在。业务库 `public.work_orders` 全程不被试跑触碰。

若未收到 40P01，响应中如实区分原因：先触发 `lock_timeout`
（SQLSTATE 55P03）、全部提交未形成环、或执行异常。

## 4. Docker Compose 启动

```bash
docker compose up --build
# API:   http://localhost:8000/docs
# PG:    localhost:5432 (postgres/postgres, 库名 patrol)
```

Compose 中 PostgreSQL 已配置 `deadlock_timeout=200ms`、
`lock_timeout=10s`、`log_lock_waits=on`。

### Swagger 试跑样例

打开 `/docs` → `POST /precheck` → `Try it out`，请求体示例下拉框
内置：AB-BA 死锁、三节点循环、同顺序安全批次三组样例，直接
`Execute` 即可。等价 curl：

```bash
curl -X POST http://localhost:8000/precheck \
  -H 'Content-Type: application/json' \
  -d '{
    "batches": [
      {"batch_id": "B1", "order_ids": ["W1", "W2"], "assignee": "alice"},
      {"batch_id": "B2", "order_ids": ["W2", "W1"], "assignee": "bob"}
    ],
    "run_replay": true
  }'
```

## 5. 本地无 Docker 时的自测

仓库提供 `scripts/local_pg.sh`：在用户目录下载并解压 PostgreSQL 15
官方二进制（免 root），初始化临时实例并监听 5433，再启动 API。

```bash
./scripts/local_pg.sh start          # 初始化/启动本地 PG + API
DATABASE_URL=postgresql://postgres@127.0.0.1:5433/patrol \
  python -m tests.selftest           # 35 项端到端自测
./scripts/local_pg.sh stop           # 停库
```

## 6. 目录结构

```
app/
  config.py     环境变量配置与业务常量（2~3 批、每批 ≤6 单）
  models.py     Pydantic 入参/出参模型与校验、Swagger 样例
  analyzer.py   等待图构造 + 有向环枚举（简化 Johnson）
  db.py         连接管理、试跑 schema 隔离、死锁实跑执行器、收尾核查
  main.py       FastAPI 路由
tests/selftest.py  端到端自测（8 个场景 35 项断言）
docker-compose.yml / Dockerfile
```

## 7. 已知边界

- 静态分析基于"行锁按工单顺序获取"这一前提；若未来改派 SQL 改为
  批量 `UPDATE ... WHERE order_id = ANY(...)`，获取顺序由规划器
  决定，需要同步调整 analyzer；
- 试跑在独立 schema 的镜像表上进行，不验证触发器/外键等业务侧
  副作用，只验证锁行为本身。
