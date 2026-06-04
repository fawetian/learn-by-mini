.PHONY: submodules update-resources update-sources status

submodules:
	git submodule update --init --recursive --depth 1

update-resources:
	git submodule update --remote --merge resource/llm-training/minimind
	git submodule update --remote --merge resource/agent/learn-claude-code
	git submodule update --remote --merge resource/agent/generic-agent
	git submodule update --remote --merge resource/task-queue/rq
	git submodule update --remote --merge resource/task-queue/asynq

update-sources: update-resources

status:
	git status --short --branch
	git submodule status
