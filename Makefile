# ============================================================
# Vision Analytics Platform — команды разработчика
#
#   make up      — поднять весь стек
#   make help    — список всех команд
#
# GPU нужен одному сервису — triton (TensorRT engine, единственный инференс
# платформы). Консьюмеры обращаются к нему по docker-сети, поэтому override-
# файлы с GPU-резервами больше не нужны: всё описано в docker-compose.yml.
# ============================================================

# Ролик-источник для бенчмарка: один и тот же между прогонами —
# иначе прогоны несравнимы (bench/README.md, «Ограничения»)
BENCH_VIDEO ?= uploads/Praha_air.mp4

COMPOSE_FILES = -f docker-compose.yml

.PHONY: help up down restart logs ps build migrate test lint bench clean

help: ## Показать список команд
	@echo "Vision Analytics Platform — команды:"
	@echo ""
	@echo "  make up       поднять весь стек (нужен NVIDIA GPU для triton)"
	@echo "  make down     остановить стек"
	@echo "  make restart  перезапустить стек"
	@echo "  make logs     логи всех сервисов (stream)"
	@echo "  make ps       статус сервисов"
	@echo "  make build    пересобрать образы"
	@echo "  make migrate  применить миграции Alembic"
	@echo "  make test     прогнать тесты (нужен локальный venv)"
	@echo "  make lint     ruff-проверка (нужен локальный venv)"
	@echo "  make bench    vision-bench: ёмкость и узкое место стримов (нужен venv + стек)"
	@echo "  make clean    остановить и удалить контейнеры/тома (ОСТОРОЖНО: данные)"

up: ## Поднять все сервисы (triton требует NVIDIA GPU)
	docker compose $(COMPOSE_FILES) up -d
	@echo ""
	@echo "Поднято. Dashboard: http://localhost:8501 | Swagger: http://localhost:8000/docs"

down: ## Остановить стек
	docker compose $(COMPOSE_FILES) down

restart: ## Перезапустить стек
	docker compose $(COMPOSE_FILES) restart

logs: ## Логи всех сервисов
	docker compose $(COMPOSE_FILES) logs -f

ps: ## Статус сервисов
	docker compose $(COMPOSE_FILES) ps

build: ## Пересобрать образы
	docker compose $(COMPOSE_FILES) build

migrate: ## Применить миграции Alembic в контейнере api
	docker compose exec api alembic upgrade head

test: ## Запустить тесты (локально, из venv)
	pytest tests/ -v --tb=short

lint: ## Проверить код ruff'ом (локально, из venv)
	ruff check app dashboard tests scripts

bench: ## vision-bench: ёмкость и узкое место стримов (нужен venv + стек)
	python -m bench run --profile default --video $(BENCH_VIDEO) --label manual

clean: ## Остановить и удалить контейнеры и тома (ДАННЫЕ БД/MinIO будут потеряны)
	docker compose $(COMPOSE_FILES) down -v
