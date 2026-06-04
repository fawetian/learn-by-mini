# Asynq 研究计划

## 研究总结

**核心价值**：Asynq 是一个基于 Redis 的 Go 分布式任务队列库。它把后台任务抽象成 `Task`，把 Redis 抽象成 `Broker`，再用 `Client`、`Server`、`processor`、`forwarder`、`recoverer`、`scheduler` 等组件拼出任务入队、并发执行、重试、调度、恢复和可观测能力。

**值得注意的设计**：

1. 任务状态全部落在 Redis key 空间里，核心状态包括待处理、执行中、定时、重试、归档、已完成和聚合中。
2. 入队、出队、重试、归档等关键状态迁移使用 Lua 脚本保证单次迁移原子性。
3. `Server` 不只是 worker 循环，而是一组后台协程的组合：处理、转发、心跳、恢复、同步、健康检查、清理、聚合。
4. Go 侧用接口收窄边界：公开层是 `Client`、`Server`、`Handler`、`ServeMux`、`Scheduler`；内部通过 `base.Broker` 隔离 Redis 实现。

## 文档索引

- [01 架构全景](01_architecture.md)
- [02 核心机制：任务生命周期](02_mechanism_task_lifecycle.md)
- [03 数据流与状态管理](03_data_flow.md)
- [04 依赖与生态](04_dependencies.md)
- [05 核心工作流](05_workflow.md)
- [06 代码阅读路径](06_learning_path.md)

## 项目概述

**它是什么**：Asynq 是一个 Go 语言后台任务队列库，使用 Redis 作为消息代理和状态存储。

**解决什么问题**：业务系统经常需要把耗时、可重试、可延迟、可横向扩展的工作从请求路径中剥离出来。没有任务队列时，开发者要自己处理任务持久化、并发执行、失败重试、进程崩溃恢复、调度和监控。

**谁在使用**：Go 服务端应用。典型用法是业务代码用 `Client` 把任务放入队列，后台进程用 `Server` 和 `Handler` 处理任务。

## 研究专题

### 专题 A：架构全景

- 目标：理解 Asynq 由哪些模块组成，每个模块是什么、为什么存在。
- 输出：[01_architecture.md](01_architecture.md)

### 专题 B：核心机制：任务生命周期

- 目标：理解任务从创建、入队、执行、成功、失败、重试到归档的完整调用链。
- 输出：[02_mechanism_task_lifecycle.md](02_mechanism_task_lifecycle.md)

### 专题 C：数据流与状态管理

- 目标：理解核心数据结构、Redis key 设计、状态迁移和一致性边界。
- 输出：[03_data_flow.md](03_data_flow.md)

### 专题 D：依赖与生态

- 目标：理解核心依赖为什么存在，以及 Asynq 在同类任务队列里的定位。
- 输出：[04_dependencies.md](04_dependencies.md)

### 专题 E：核心工作流

- 目标：追踪入队、消费、调度、恢复、聚合等端到端流程。
- 输出：[05_workflow.md](05_workflow.md)

### 专题 F：学习路径

- 目标：总结推荐阅读顺序，帮助后续按目标继续拆解源码。
- 输出：[06_learning_path.md](06_learning_path.md)

## 待解决疑问

1. README 明确提到部分 Lua 脚本可能不兼容 Redis Cluster，需要后续单独研究哪些脚本受影响、哪些 key 使用 hash tag 规避了跨槽问题。
2. `BatchEnqueue` 文档说明批量入队不是整体事务，需要后续结合测试确认调用方应该如何处理部分成功。
3. 聚合任务的失败补偿路径比较长，后续可以单独研究 `AggregationCheck`、`ReadAggregationSet`、`DeleteAggregationSet` 与 recoverer 的配合。
