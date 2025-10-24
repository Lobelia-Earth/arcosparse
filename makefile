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

dev-test: ## Run one full run, for development
	poetry run python test_script.py

publish: ## Publish package to PyPi
	poetry publish --build --username __token__ --password "$(TOKEN)" 

install-poetry: ## Install poetry
	pip install poetry==2.2.0

install: install-poetry ## Install dependencies
	poetry install

bump-version-patch: ## Bump version patch
	poetry version patch

bump-version-minor: ## Bump version minor
	poetry version minor

bump-version-major: ## Bump version major
	poetry version major

update-snapshots: ## Update snapshot
	poetry run python -m pytest --snapshot-update tests/test_get_entities.py
	poetry run python -m pytest --snapshot-update tests/test_get_metadata.py