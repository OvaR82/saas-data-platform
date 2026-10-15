# Usage: make <target> [ENV=dev] [TENANT=all] [START=2025-01-01] [END=2025-06-30] [LAYERS=bronze]
ENV ?= dev
TENANT ?= all
START ?= 2025-01-01
END ?= 2025-06-30
LAYERS ?= bronze

.PHONY: help setup lint format test validate-config run clean

help:
	@grep -E '^[a-z-]+:' Makefile | cut -d: -f1

setup:  ## Create .venv with pinned versions and install git hooks
	uv sync
	uv run pre-commit install

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

test:
	uv run pytest

validate-config:
	uv run saas-pipeline validate-config

run:
	uv run saas-pipeline run --env $(ENV) --tenant $(TENANT) \
		--start-date $(START) --end-date $(END) --layers $(LAYERS)

clean:  ## Remove generated layers (keeps data/raw)
	rm -rf data/bronze data/silver data/gold data/*_quarantine data/shared data/qa data/main
