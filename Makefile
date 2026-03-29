.DEFAULT_GOAL := help

.PHONY: help sync init-env collect collect-repo peek process process-repo merge push pipeline check

help:
	@printf "Available targets:\n"
	@printf "  make sync         - Install project dependencies with uv\n"
	@printf "  make init-env     - Create .env from .env.example (if missing)\n"
	@printf "  make collect      - Collect all repos from config/repos.py\n"
	@printf "  make collect-repo - Collect one repo (usage: make collect-repo REPO=owner/repo)\n"
	@printf "  make peek         - GraphQL one-sample preview from pola-rs/polars (no file writes)\n"
	@printf "  make process      - Process all raw files into messages[]\n"
	@printf "  make process-repo - Process one repo (usage: make process-repo REPO=owner/repo)\n"
	@printf "  make merge        - Merge processed files and write train/test split\n"
	@printf "  make push         - Push train/test dataset to HuggingFace\n"
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
	uv run python scripts/run_collect.py

collect-repo:
	@if [ -z "$(REPO)" ]; then \
		printf "Usage: make collect-repo REPO=owner/repo\n"; \
		exit 1; \
	fi
	uv run python scripts/run_collect.py --repo $(REPO)

peek:
	uv run python scripts/run_peek.py

process:
	uv run python scripts/run_process.py

process-repo:
	@if [ -z "$(REPO)" ]; then \
		printf "Usage: make process-repo REPO=owner/repo\n"; \
		exit 1; \
	fi
	uv run python scripts/run_process.py --repo $(REPO)

merge:
	uv run python scripts/run_merge.py

push:
	uv run python scripts/run_push.py

pipeline: collect process merge push

check:
	python3 -m compileall src scripts config
