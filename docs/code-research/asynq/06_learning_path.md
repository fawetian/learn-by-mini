# 代码阅读路径

## 快速理解

1. `resource/task-queue/asynq/README.md` — 先理解项目目标和使用方式。读完后知道 Asynq 是什么、支持哪些能力、典型调用姿势是什么。
2. `resource/task-queue/asynq/asynq.go` — 读公开核心类型。读完后理解 `Task`、`TaskInfo`、任务状态和 Redis 连接配置。
3. `resource/task-queue/asynq/client.go` — 读任务如何进入系统。读完后理解入队选项、唯一任务、定时任务、分组任务。
4. `resource/task-queue/asynq/server.go` — 读 server 如何组装运行时组件。读完后理解 Asynq 为什么不是一个简单 worker 循环。
5. `resource/task-queue/asynq/processor.go` — 读任务如何被取出并执行。读完后能串起并发控制、lease、context、成功、重试、归档。
6. `resource/task-queue/asynq/internal/base/base.go` 和 `resource/task-queue/asynq/internal/rdb/rdb.go` — 读 Redis 状态模型和原子迁移。读完后理解真正的可靠性边界在哪里。

## 按目标索引

### 想理解整体架构

- `server.go:431`：Server 构造和后台组件组装。
- `server.go:680`：Server 启动所有组件。
- `internal/base/base.go:698`：Broker 接口定义。
- `internal/rdb/rdb.go:28`：Redis 实现入口。

### 想理解任务入队

- `client.go:272`：选项合并和校验。
- `client.go:385`：`EnqueueContext` 主流程。
- `client.go:600`：立即、定时、分组三种路径的内部 helper。
- `internal/rdb/rdb.go:98`：立即入队 Lua 脚本。
- `internal/rdb/rdb.go:771`：定时任务写入 scheduled。

### 想理解任务消费

- `processor.go:93`：processor 初始化。
- `processor.go:170`：出队并启动 worker。
- `processor.go:205`：为 handler 注入 context。
- `processor.go:276`：成功处理。
- `processor.go:335`：失败处理。
- `processor.go:424`：handler panic 捕获。

### 想理解可靠性设计

- `internal/rdb/rdb.go:356`：出队时 pending -> active + lease。
- `heartbeat.go:144`：心跳写状态并延长 lease。
- `recoverer.go:84`：lease 过期恢复。
- `syncer.go:14`：状态同步失败后的补偿重试。
- `internal/rdb/rdb.go:916`：失败重试状态迁移。
- `internal/rdb/rdb.go:1018`：归档状态迁移。

### 想理解调度和周期任务

- `forwarder.go:15`：到期任务转发。
- `internal/rdb/rdb.go:1053`：scheduled/retry 到 pending 的 Redis 迁移。
- `scheduler.go:181`：cron 到入队动作。
- `scheduler.go:208`：注册周期任务。
- `periodic_task_manager.go:49`：动态同步周期任务配置。

### 想理解扩展和业务集成

- `server.go:638`：`Handler` 接口。
- `servemux.go:29`：任务类型路由器。
- `servemux.go:146`：中间件机制。
- `server.go:277`：错误处理器扩展。
- `server.go:257`：分组聚合扩展。
- `asynq.go:565`：任务结果写入器。

## 值得深入学习的代码片段

1. `processor.go:170` 到 `processor.go:257`：一个紧凑但完整的 worker 执行循环，包含并发限流、出队、lease、context、handler 执行和四种退出路径。
2. `internal/rdb/rdb.go:98` 到 `internal/rdb/rdb.go:108`：最小入队原子操作，展示任务 hash 和 pending list 如何一起更新。
3. `internal/rdb/rdb.go:884` 到 `internal/rdb/rdb.go:948`：失败重试迁移，展示如何同时更新 active、lease、retry、任务消息和统计。
4. `heartbeat.go:144` 到 `heartbeat.go:201`：心跳不仅写可观测状态，还负责延长任务租约。
5. `aggregator.go:130` 到 `aggregator.go:174`：把分组任务读取、业务聚合、重新入队、删除聚合集合串起来。

## 令人困惑的地方

1. **为什么 processor 不用阻塞 pop**：第一眼看会觉得轮询 Redis 浪费。结合多队列优先级、暂停队列、scheduled/retry 转发后会发现，轮询让队列选择逻辑集中在 Go 侧，但代价是 Redis 负载。
2. **为什么有 syncer**：第一次读成功/失败处理时会疑惑为什么 Redis 写失败不直接返回。原因是 handler 已经执行完，任务状态必须尽量补偿，否则会卡在 active。
3. **为什么 Task 和 TaskMessage 分开**：公开 API 要简单，内部状态机要完整。二者分离后，业务不需要关心重试次数、lease、归档时间等系统字段。
4. **为什么完成任务默认删除**：任务队列核心目标是可靠执行，不是长期存储结果。保留结果是额外成本，所以通过 retention 显式开启。
