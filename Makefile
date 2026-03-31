.DEFAULT_GOAL := help

.PHONY: help sync init-env collect collect-repo peek process process-repo merge push publish pipeline check

help:
	@printf "Available targets:\n"
	@printf "  make sync         - Install project dependencies with uv\n"
	@printf "  make init-env     - Create .env from .env.example (if missing)\n"
	@printf "  make collect      - Collect all repos\n"
	@printf "  make collect-repo - Collect one repo (usage: make collect-repo REPO=owner/repo)\n"
	@printf "  make peek         - GraphQL one-sample preview from pola-rs/polars (no file writes)\n"
	@printf "  make process      - Process all raw files into messages[]\n"
	@printf "  make process-repo - Process one repo (usage: make process-repo REPO=owner/repo)\n"
	@printf "  make merge        - Merge processed files and write train/test split\n"
	@printf "  make push         - Push train/test dataset to HuggingFace\n"
	@printf "  make publish      - Build and publish package to PyPI\n"
	@printf "  make pipeline     - Run collect, process, merge, then push\n"
	@printf "  make check        - Compile Python files for a quick syntax check\n"

sync:
	uv sync

init-env:
	@if [ -f .env ]; then \
		printf ".env already exists\n"; \
	else \
		cp .env.example .env && printf "Created .env from .env.example\n"; \
	fi

collect:
	uv run patch-sft collect

collect-repo:
	@if [ -z "$(REPO)" ]; then \
		printf "Usage: make collect-repo REPO=owner/repo\n"; \
		exit 1; \
	fi
	uv run patch-sft collect --repo $(REPO)

peek:
	uv run patch-sft peek

process:
	uv run patch-sft process

process-repo:
	@if [ -z "$(REPO)" ]; then \
		printf "Usage: make process-repo REPO=owner/repo\n"; \
		exit 1; \
	fi
	uv run patch-sft process --repo $(REPO)

merge:
	uv run patch-sft merge

push:
	uv run patch-sft push

publish:
	rm -rf dist/
	uv build
	uv publish

pipeline: collect process merge push

check:
	python3 -m compileall src
