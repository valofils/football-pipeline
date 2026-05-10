.PHONY: lint test build up down smoke clean help

PYTHON        := python3
IMAGE_NAME    := football-pipeline-airflow
COMPOSE_FILE  := docker-compose.yaml
MARQUEZ_URL   := http://localhost:5002/api/v1
COVERAGE_MIN  := 80

help:          ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

lint:          ## Run ruff and black --check on all Python files
	ruff check .
	black --check .

lint-fix:      ## Auto-fix ruff and black formatting
	ruff check --fix .
	black .

test:          ## Run pytest with coverage (gate ≥ $(COVERAGE_MIN)%)
	pytest tests/ \
		--cov=. \
		--cov-report=term-missing \
		--cov-report=xml \
		--cov-fail-under=$(COVERAGE_MIN) \
		-v

test-fast:     ## Run pytest without coverage (faster feedback)
	pytest tests/ -v -x

build:         ## Build the custom Airflow Docker image
	docker build -t $(IMAGE_NAME):local .

build-no-cache: ## Build image without Docker layer cache
	docker build --no-cache -t $(IMAGE_NAME):local .

up:            ## Start the full stack via Docker Compose
	docker compose -f $(COMPOSE_FILE) up -d --wait --timeout 120

down:          ## Stop and remove all containers and volumes
	docker compose -f $(COMPOSE_FILE) down -v

logs:          ## Tail logs from all services
	docker compose -f $(COMPOSE_FILE) logs -f

smoke:         ## POST a lineage event to Marquez and assert HTTP 201
	@echo "Running smoke test against $(MARQUEZ_URL)/lineage …"
	@HTTP_STATUS=$$(curl -s -o /tmp/marquez_smoke.json -w "%{http_code}" \
		-X POST $(MARQUEZ_URL)/lineage \
		-H "Content-Type: application/json" \
		-d '{ \
			"eventType": "START", \
			"eventTime": "2024-01-01T00:00:00Z", \
			"run": {"runId": "smoke-test-run-00000000-0000-0000-0000-000000000001"}, \
			"job": {"namespace": "football_pipeline", "name": "smoke_test_job"}, \
			"inputs": [], \
			"outputs": [], \
			"producer": "https://github.com/football-pipeline/makefile" \
		}') && \
	echo "Response: $$(cat /tmp/marquez_smoke.json)" && \
	if [ "$$HTTP_STATUS" != "201" ]; then \
		echo "FAIL: expected 201, got $$HTTP_STATUS"; exit 1; \
	fi && \
	echo "PASS: Marquez returned 201"

ci:            ## Run the full local CI sequence: lint → test → build → up → smoke → down
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) build
	$(MAKE) up
	$(MAKE) smoke
	$(MAKE) down

clean:         ## Remove Python cache files and coverage artefacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -f coverage.xml .coverage
	rm -f /tmp/marquez_smoke.json /tmp/marquez_response.json
