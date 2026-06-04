# 依赖与生态

## 核心依赖

### Redis

**它是什么**：Asynq 的消息代理和状态存储。

**为什么选择它**：任务队列需要快速读写、可持久化、可跨进程共享的数据结构。Redis 同时提供 list、zset、hash、set、pubsub 和 Lua 脚本，刚好覆盖 pending 队列、延迟集合、任务详情、全局索引、取消通知和原子状态迁移。

**项目中的角色**：所有任务状态、队列状态、server 心跳、worker 信息、scheduler 条目、结果数据都落在 Redis 里。

### go-redis

**它是什么**：Go Redis 客户端。

**为什么选择它**：它提供普通 Redis、Sentinel、Cluster 的统一客户端抽象。Asynq 的 `RedisConnOpt` 会把不同连接配置转换成 go-redis client，见 `asynq.go:254` 到 `asynq.go:464`。

**项目中的角色**：`rdb.RDB` 通过 go-redis 执行命令、pipeline、Lua 脚本和 pubsub。

### protobuf

**它是什么**：结构化二进制序列化。

**为什么选择它**：任务内部消息需要稳定编码，字段多且需要演进。`TaskMessage` 通过 protobuf 写入 Redis，见 `internal/base/base.go:302`。

**项目中的角色**：序列化任务消息、server 信息、worker 信息、scheduler 事件等内部数据。

### robfig/cron

**它是什么**：Go 里的 cron 调度库。

**为什么选择它**：周期调度本身不是 Asynq 的核心差异，使用成熟 cron 库能把重点留给任务入队和状态管理。

**项目中的角色**：`Scheduler` 用它注册 cron 表达式，触发时调用 `Client.Enqueue`，见 `scheduler.go:181` 和 `scheduler.go:208`。

### x/time/rate

**它是什么**：限流工具。

**为什么选择它**：当 Redis 异常或出队持续失败时，不能刷爆日志。processor 使用 rate limiter 限制错误日志，见 `processor.go:50` 和 `processor.go:188`。

**项目中的角色**：保护日志系统，降低故障时的噪声。

## 有意不使用的方案

### 不把业务编码方式固定死

Asynq 的 `Task.Payload` 是 `[]byte`，没有强制 JSON、gob 或 protobuf。这样让业务自由选择编码方式。代价是库本身不理解业务 payload，也无法做更强的 schema 校验。

### 不把 broker 抽象成通用消息队列协议

虽然内部有 `Broker` 接口，但它的方法是任务队列语义，不是通用消息系统语义。它包含 lease、retry、archive、aggregation、scheduler state 等 Asynq 需要的操作。这说明 `Broker` 主要用于内部解耦和测试，而不是承诺支持任意后端。

### 不默认保留成功任务结果

成功任务默认删除，只有设置 retention 才保留 completed 状态和结果。这样默认成本低，但如果业务需要审计或结果查询，就必须显式配置。

## 外部系统集成

**集成点在哪里**：

- Redis：核心状态存储和消息代理。
- Sentinel/Cluster：通过不同 Redis 连接选项接入高可用部署。
- Prometheus 和 Web UI：README 提到用于指标和队列管理，但核心库主要提供状态与 inspector。
- CLI：`tools/asynq` 用于检查和控制队列。

**为什么这样划分边界**：Asynq 是库，不是完整任务平台。它提供运行时和状态模型，把部署、监控面板、业务任务编码交给使用方或附属工具。

## 项目定位

**在同类工具中的位置**：Asynq 更像 Go 生态里的轻量 Celery/RQ，但它的并发模型天然贴合 goroutine，并提供较完整的生产能力：优先级、多队列、延迟、重试、唯一任务、任务聚合、周期任务、租约恢复、心跳、检查接口。

**有意不支持的功能**：

1. 不保证精确一次。它强调至少执行一次，业务处理函数需要自己做幂等。
2. 不把 Redis Cluster 作为完全无差别后端。README 明确提示部分 Lua 脚本可能不兼容 Redis Cluster。
3. 不隐藏所有 Redis 语义。队列暂停、lease、延迟 zset、状态统计都与 Redis 数据结构强相关。
