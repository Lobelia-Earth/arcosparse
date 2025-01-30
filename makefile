## Project documentation here
.DEFAULT_GOAL := help

# Print help message
help:
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

format: ## Apply formatting
	poetry run black src/
	poetry run isort src/

check-format: ## Apply formatting, check only
	poetry run black --check src/
	poetry run isort --check-only src/
	poetry run flake8 src/
	poetry run pyright src/
	poetry run bandit -r src/

test: ## Run tests
	poetry run pytest tests/
