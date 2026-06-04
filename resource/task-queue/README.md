# 任务队列

这个目录用于收集和走读任务队列相关的复杂系统样本。

## 当前样本

- [RQ](rq)：基于 Redis/Valkey 的 Python 任务队列。
- [Asynq](asynq)：基于 Redis 的 Go 分布式任务队列。

## 学习重点

1. 任务如何入队、序列化和持久化。
2. Worker 如何获取任务、执行任务并记录结果。
3. 失败任务如何重试、延迟和进入失败队列。
4. 定时任务、重复任务和 cron 调度如何实现。
5. 多队列优先级、worker pool 和横向扩展。
6. Redis/Valkey 在队列、状态机和注册表中的角色。
7. Python 和 Go 任务队列在 API、并发模型和可靠性设计上的差异。
8. 一个轻量任务队列库如何保持简单 API，同时覆盖生产常见场景。

## 第一轮源码入口

RQ：

1. `rq/queue.py`：入队 API 和队列抽象。
2. `rq/job.py`：任务对象、状态和序列化。
3. `rq/worker.py`：worker 主循环和任务执行。
4. `rq/registry.py`：不同任务状态的注册表。
5. `rq/results.py`：任务结果保存。
6. `rq/scheduler.py`、`rq/cron.py`、`rq/repeat.py`：调度、cron 和重复任务。
7. `tests/`：从测试用例反推行为边界。

Asynq：

1. `client.go`：任务入队和客户端 API。
2. `server.go`：任务处理服务器的配置和启动。
3. `processor.go`：任务拉取、并发执行和生命周期处理。
4. `scheduler.go`、`periodic_task_manager.go`：调度和周期任务。
5. `aggregator.go`：任务聚合和批处理。
6. `inspector.go`：队列和任务的可观测与管理接口。
7. `*_test.go`：从测试用例理解可靠性边界和状态迁移。
