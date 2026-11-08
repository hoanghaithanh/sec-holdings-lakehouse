.PHONY: lint format typecheck test validate check up up-uat down

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy

test:
	uv run pytest

validate:
	uv run dagster definitions validate -m sec_lakehouse.definitions

check: lint typecheck test validate

up:
	docker compose up --build

up-uat:
	docker compose -f docker-compose.yml -f docker-compose.uat.yml --env-file .env.uat up -d --pull always

down:
	docker compose down
