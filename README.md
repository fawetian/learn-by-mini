# learn-by-mini

通过学习和重建足够小、足够具体的样本，理解复杂系统和复杂事务。
当前第一批样本聚焦 LLM、agent 和自进化系统。

## 资源分类

- [LLM 训练](resource/llm-training)：从零理解模型训练流水线。
  - [MiniMind](resource/llm-training/minimind)：从零开始的小型 LLM 训练流水线。
- [Agent](resource/agent)：理解 agent 运行框架、自主执行和自进化机制。
  - [Learn Claude Code](resource/agent/learn-claude-code)：受 Claude Code 启发的轻量
    agent 运行框架和教程序列。
  - [GenericAgent](resource/agent/generic-agent)：极简自进化 agent 框架，包含小型
    agent 循环、原子工具、记忆系统和技能固化机制。
- [任务队列](resource/task-queue)：理解后台任务、队列、worker 和调度。
  - [RQ](resource/task-queue/rq)：基于 Redis/Valkey 的 Python 任务队列。
  - [Asynq](resource/task-queue/asynq)：基于 Redis 的 Go 分布式任务队列。

这些样本项目都以 Git 子模块的形式放在 `resource/` 下。

## 目录结构

```text
resource/                按主题归档的复杂系统样本
tutorials/               分步骤教程和本地走读记录
notes/                   阅读笔记、设计笔记和问题记录
experiments/             可在本地运行的小实验和原型
```

## 初始化

克隆仓库并拉取 Git 子模块：

```bash
git clone --recurse-submodules <repo-url>
```

如果已经克隆了仓库，但还没有拉取 Git 子模块：

```bash
make submodules
```

更新上游主题资源：

```bash
make update-resources
```

## 学习路线

1. `tutorials/minimind`：理解 LLM 训练栈，包括分词器、数据、模型、训练循环、
   对齐、评估和推理。
2. `tutorials/learn-claude-code`：理解 agent 产品的运行框架，包括循环设计、工具调用、
   权限、钩子、记忆、子 agent 和上下文管理。
3. `tutorials/generic-agent`：理解自进化 agent，包括最小系统控制、原子工具、分层记忆、
   技能沉淀和重复任务优化。
4. `experiments`：先重建每个子系统最小可用版本，再和上游实现对比。
