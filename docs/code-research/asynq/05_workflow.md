# 核心工作流

## 端到端业务流程

### 流程一：立即任务入队

**它是什么**：业务代码把一个任务放入队列，等待 worker 处理。

**触发方式**：调用 `Client.Enqueue` 或 `Client.EnqueueContext`。

```mermaid
sequenceDiagram
    participant 业务代码
    participant Client
    participant Broker
    participant RDB
    participant Redis

    业务代码->>Client: Enqueue(task, opts)
    Client->>Client: 合并并校验选项
    Client->>Client: 构造 TaskMessage
    Client->>Broker: Enqueue
    Broker->>RDB: 调用 Redis 实现
    RDB->>Redis: Lua 写任务 hash 并 LPUSH pending
    Redis-->>RDB: 成功或冲突
    RDB-->>Client: 返回结果
    Client-->>业务代码: TaskInfo
```

| 步骤 | 负责模块 | 做什么 | 为什么在这一步 | 代码位置 |
|------|----------|--------|----------------|----------|
| 1 | Client | 检查 task 是否为空、类型是否为空 | 先在本地拒绝明显非法输入 | `client.go:385` |
| 2 | Client | 合并任务默认选项和入队选项 | 支持创建时默认、入队时覆盖 | `client.go:392` |
| 3 | Client | 构造 `TaskMessage` | 转成内部持久化模型 | `client.go:414` |
| 4 | RDB | 写任务 hash 和 pending list | 原子创建任务状态 | `internal/rdb/rdb.go:98` |

### 流程二：worker 消费任务

**它是什么**：Server 从队列取任务，并发执行 handler。

**触发方式**：调用 `Server.Start` 或 `Server.Run`。

```mermaid
sequenceDiagram
    participant Server
    participant Processor
    participant RDB
    participant Handler
    participant Heartbeater
    participant Redis

    Server->>Processor: start
    Processor->>RDB: Dequeue
    RDB->>Redis: pending -> active + lease
    Redis-->>RDB: TaskMessage
    RDB-->>Processor: 任务和 lease 截止时间
    Processor->>Heartbeater: worker starting
    Processor->>Handler: ProcessTask
    Handler-->>Processor: 成功或失败
    Processor->>RDB: Done / MarkAsComplete / Retry / Archive
    Processor->>Heartbeater: worker finished
```

| 步骤 | 负责模块 | 做什么 | 为什么在这一步 | 代码位置 |
|------|----------|--------|----------------|----------|
| 1 | Server | 组装并启动所有后台组件 | 处理任务需要多个协程协作 | `server.go:680` |
| 2 | Processor | 用 semaphore 控制并发 | 避免超过配置的 worker 数 | `processor.go:174` |
| 3 | RDB | 出队并创建 lease | 防止 worker 崩溃后任务永久丢失 | `internal/rdb/rdb.go:356` |
| 4 | Processor | 构造带 metadata 的 context | handler 能读取任务 ID、队列、重试次数 | `processor.go:205` |
| 5 | Processor | 调用 handler 并捕获 panic | 统一成功、失败和 panic 处理 | `processor.go:424` |

### 流程三：定时与重试任务转发

**它是什么**：把 scheduled 和 retry 中到期的任务移回 pending。

**触发方式**：Server 启动后 forwarder 定时执行。

```mermaid
sequenceDiagram
    participant Forwarder
    participant RDB
    participant Redis
    participant Processor

    Forwarder->>RDB: ForwardIfReady
    RDB->>Redis: 扫描 scheduled 和 retry 中已到期任务
    Redis->>Redis: 到期任务转 pending 或 aggregating
    Processor->>RDB: 后续 Dequeue
```

| 步骤 | 负责模块 | 做什么 | 为什么在这一步 | 代码位置 |
|------|----------|--------|----------------|----------|
| 1 | Forwarder | 按间隔触发检查 | 避免 processor 关心延迟集合 | `forwarder.go:55` |
| 2 | RDB | 每轮最多移动一批任务 | 控制 Lua 脚本运行时间 | `internal/rdb/rdb.go:1071` |
| 3 | RDB | 分组任务转 aggregating，普通任务转 pending | 复用同一转发器处理两类任务 | `internal/rdb/rdb.go:1076` |

### 流程四：worker 崩溃恢复

**它是什么**：发现 active 但 lease 过期的任务，并转入 retry 或 archive。

**触发方式**：recoverer 定时扫描。

```mermaid
flowchart TD
    A[recoverer 定时执行] --> B[查找 lease 过期任务]
    B --> C{是否还有重试次数}
    C -->|有| D[进入 retry]
    C -->|没有| E[进入 archive]
    D --> F[等待 forwarder 到期转 pending]
    E --> G[保留失败记录供检查]
```

| 异常场景 | 触发条件 | 处理方式 | 对用户的影响 |
|----------|----------|----------|--------------|
| worker 崩溃 | lease 过期 | recoverer 调用 Retry 或 Archive | 任务不会永久卡在 active |
| 关机超时 | shutdown timeout 到期 | processor 调用 Requeue | 任务重新回 pending 等待后续处理 |
| Redis 状态写失败 | Done/Retry/Archive 调用失败 | syncer 缓存补偿操作并重试 | 降低状态迁移失败造成的卡死风险 |

### 流程五：周期任务产生

**它是什么**：按 cron 表达式生成任务并入队。

**触发方式**：Scheduler 注册任务后启动。

```mermaid
sequenceDiagram
    participant 业务代码
    participant Scheduler
    participant Cron
    participant Client
    participant RDB

    业务代码->>Scheduler: Register(cronspec, task)
    Scheduler->>Cron: AddJob
    Cron->>Scheduler: 到点执行 enqueueJob
    Scheduler->>Client: Enqueue
    Client->>RDB: 写入队列
    Scheduler->>RDB: 记录调度历史
```

## 模块内部执行流程

### processor 内部流程

**它是什么**：worker 主循环，负责“取任务、执行、写状态”。

**为什么需要详细了解**：它是任务生命周期的中心，连接 Broker、Handler、heartbeater、syncer、recoverer。

```mermaid
flowchart TD
    A[processor.start] --> B[循环调用 exec]
    B --> C{获取并发令牌}
    C -->|成功| D[按优先级生成队列列表]
    D --> E[Broker.Dequeue]
    E -->|无任务| F[睡眠后释放令牌]
    E -->|出错| G[限流记录错误并释放令牌]
    E -->|取到任务| H[创建 lease 和 context]
    H --> I[启动 handler goroutine]
    I --> J{等待结果}
    J -->|成功| K[Done 或 MarkAsComplete]
    J -->|失败| L[Retry 或 Archive]
    J -->|abort| M[Requeue]
    J -->|lease 过期| L
```

### Server 启停流程

**它是什么**：Server 管理所有后台组件的生命周期。

**为什么需要详细了解**：任务队列进程退出时如果顺序不对，会造成任务丢失或状态不同步。源码在 `server.go:735` 注释里强调关闭顺序重要。

```mermaid
flowchart TD
    A[Start] --> B[设置 handler]
    B --> C[切换 server 状态为 active]
    C --> D[启动 heartbeater]
    C --> E[启动 healthchecker]
    C --> F[启动 subscriber]
    C --> G[启动 syncer]
    C --> H[启动 recoverer]
    C --> I[启动 forwarder]
    C --> J[启动 processor]
    C --> K[启动 janitor]
    C --> L[启动 aggregator]
    M[Shutdown] --> N[先停发送方]
    N --> O[等待 worker]
    O --> P[清理状态并关闭 Redis]
```

## 流程间的关联

```mermaid
graph LR
    入队 --> Pending
    Scheduler --> 入队
    Forwarder --> Pending
    Pending --> Processor
    Processor --> Handler
    Handler --> 成功状态
    Handler --> 失败状态
    失败状态 --> Retry
    Retry --> Forwarder
    Processor --> Heartbeater
    Heartbeater --> Lease
    Lease --> Recoverer
    Recoverer --> Retry
    Recoverer --> Archive
    Aggregator --> 入队
    Inspector --> Redis状态
```
