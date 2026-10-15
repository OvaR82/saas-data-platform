# Usage: make <target> [ENV=dev] [TENANT=all] [START=2025-01-01] [END=2025-06-30] [LAYERS=bronze,silver,gold]
ENV ?= dev
TENANT ?= all
START ?= 2025-01-01
END ?= 2025-06-30
LAYERS ?= bronze,silver,gold
# Same default as config/base.yaml (paths.data_root)
DATA_ROOT ?= $(or $(SAAS_DATA_ROOT),data)

.PHONY: help setup lint format test validate-config run notebook clean

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

notebook:  ## Open the demo notebook (Jupyter runs in WSL, open the printed URL in the browser)
	uv run --group demo jupyter lab --no-browser --notebook-dir=notebooks

clean:  ## Remove generated layers under DATA_ROOT (never touches data/raw)
	rm -rf $(addprefix $(DATA_ROOT)/,bronze silver gold bronze_quarantine silver_quarantine shared mentoring qa main)
