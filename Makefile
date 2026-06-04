.DEFAULT_GOAL := help
SERVICE_DIR := order-service

.PHONY: help build test up down logs ps health clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build: ## Build the order-service jar locally (mvn package)
	mvn -f $(SERVICE_DIR)/pom.xml -B clean package

test: ## Run unit/slice tests locally (mvn test)
	mvn -f $(SERVICE_DIR)/pom.xml -B test

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
