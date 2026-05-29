
.PHONY: help install unit lint up down integration e2e load slo all clean

help:
	@echo "Targets:"
	@echo "  install      - create venv and install runtime + test deps"
	@echo "  unit         - run unit tests (no Docker required)"
	@echo "  lint         - run ruff over source and tests"
	@echo "  up           - docker compose up -d --build --wait"
	@echo "  down         - docker compose down -v --remove-orphans"
	@echo "  integration  - pytest tests/integration (requires `up`)"
	@echo "  e2e          - pytest tests/e2e (requires `up`)"
	@echo "  load         - k6 run load/smoke.js (requires `up`)"
	@echo "  slo          - python scripts/check_slo.py (requires `up`)"
	@echo "  all          - unit + up + integration + e2e + load + slo + down"
	@echo "  clean        - rm -rf .pytest_cache .ruff_cache artifacts"

install:
	python -m venv .venv
	. .venv/Scripts/activate && pip install --upgrade pip && pip install -r requirements.txt

unit:
	pytest tests/unit -v

lint:
	ruff check app consumer_service wms_service scripts tests --select=E,F,B --ignore=E501,B008

up:
	docker compose up -d --build --wait --wait-timeout 300

down:
	docker compose down -v --remove-orphans

integration:
	pytest tests/integration -v

e2e:
	pytest tests/e2e -v

load:
	mkdir -p artifacts
	k6 run load/smoke.js -e WMS_URL=http://localhost:8080 -e LOAD_DURATION=45s --summary-export=artifacts/k6-summary.json

slo:
	python scripts/check_slo.py --prom http://localhost:9090 --slo monitoring/slo.yml --wait 30

all: unit up integration e2e load slo down

clean:
	rm -rf .pytest_cache .ruff_cache artifacts
