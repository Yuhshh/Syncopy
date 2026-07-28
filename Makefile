.PHONY: install test lint format typecheck check run db-up db-down migrate

install:
	python -m pip install -e ".[dev,live,backtest]"

test:
	pytest

lint:
	ruff check .

format:
	ruff format .
	ruff check --fix .

typecheck:
	mypy src

check: lint test

run:
	syncopy run

db-up:
	docker compose up -d postgres

db-down:
	docker compose down

migrate:
	alembic upgrade head

