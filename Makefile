.PHONY: up down logs test reset-data

# Start all services
up:
	docker-compose up --build -d

# Stop all services
down:
	docker-compose down

# View logs for a specific service or all services
logs:
	docker-compose logs -f $(service)

# Run tests
test:
	pytest tests/ -v

# Reset all data (databases, volumes, raw/processed data)
reset-data: down
	docker volume rm cryptoflow_postgres-data cryptoflow_mlflow-artifacts || true
	rm -rf data/raw/* data/processed/* data/features/* ml/models/*
	@echo "All data reset successfully."
