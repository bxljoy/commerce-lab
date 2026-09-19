.DEFAULT_GOAL := help
SERVICE_DIR := order-service

.PHONY: help build test test-order test-inventory verify verify-order verify-inventory verify-restart verify-inventory-image verify-outbox-recovery up down logs ps health clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Build the order-service jar locally (mvn package)
	mvn -f $(SERVICE_DIR)/pom.xml -B clean package

test: test-order test-inventory ## Run both services' unit + slice tests (Surefire; fast, no Docker)

test-order: ## Run order-service unit + slice tests
	mvn -f order-service/pom.xml -B test

test-inventory: ## Run inventory-service unit + slice tests
	mvn -f inventory-service/pom.xml -B test

verify: verify-order verify-inventory ## Run both services' full test suites (needs Docker)

verify-order: ## Run all order-service tests including Testcontainers integration tests
	mvn -f order-service/pom.xml -B verify

verify-inventory: ## Run all inventory-service tests including Testcontainers integration tests
	mvn -f inventory-service/pom.xml -B verify

verify-restart: ## Build Compose stack and prove an order survives service restart
	bash scripts/verify-order-restart.sh

verify-inventory-image: ## Build and smoke-test the inventory-service image
	bash scripts/verify-inventory-service.sh

verify-outbox-recovery: ## Prove committed outbox recovery and duplicate publication after process kill
	bash scripts/verify-outbox-recovery.sh

up: ## Build images and start the stack (detached)
	docker compose up --build -d

down: ## Stop and remove the stack
	docker compose down

logs: ## Tail logs from all services
	docker compose logs -f

ps: ## Show running services and their health
	docker compose ps

health: ## Curl the order-service health endpoint from the host
	curl -fsS http://localhost:8080/actuator/health | python3 -m json.tool

clean: ## Remove build artifacts
	mvn -f $(SERVICE_DIR)/pom.xml -B clean
