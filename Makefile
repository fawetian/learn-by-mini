.PHONY: submodules update-sources status

submodules:
	git submodule update --init --recursive --depth 1

update-sources:
	git submodule update --remote --merge sources/minimind
	git submodule update --remote --merge sources/learn-claude-code
	git submodule update --remote --merge sources/generic-agent

status:
	git status --short --branch
	git submodule status
