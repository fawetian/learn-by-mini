# Asynq 本机 Redis 示例

这个示例直接连接本机 Redis，不使用模拟实现。它包含两个命令：

- `worker`：启动 Asynq worker，从 Redis 消费任务。
- `enqueue`：提交一个立即执行的审计任务，以及一个 10 秒后执行的日报任务。

注意：`resource/task-queue/asynq` 是第三方 submodule。这个学习示例放在 `docs/code-research/asynq/example`，这样父仓库可以正常追踪、提交和推送，不需要改写上游项目源码。

## 运行

先启动 worker：

```sh
go run . worker
```

另开一个终端提交任务：

```sh
go run . enqueue -user 42 -action signup
```

默认连接 `127.0.0.1:6379`。如果需要改 Redis 配置：

```sh
REDIS_ADDR=127.0.0.1:6379 REDIS_DB=0 go run . worker
```

任务执行结果会写入当前目录下的 `runtime/`：

- `runtime/audit.log`：审计任务追加的 JSON 行。
- `runtime/report-YYYY-MM-DD.txt`：延迟日报任务生成的文件。

## 观察点

1. 先运行 `enqueue` 再运行 `worker`，任务会留在 Redis 中，worker 启动后继续消费。
2. 日报任务使用 `asynq.ProcessIn(10 * time.Second)`，会先进入定时集合，再由 Asynq 转入待处理队列。
3. 审计任务和日报任务都设置了 `MaxRetry`、`Timeout`、`Retention`，这些选项是业务集成时最常用的控制点。
