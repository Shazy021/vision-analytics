# ============================================================
# Vision Analytics Platform — команды разработчика
#
#   make up      — поднять весь стек (GPU определяется автоматически)
#   make help    — список всех команд
#
# Авто-детект GPU: если nvidia-smi доступен и видит карту, up добавляет
# override-файл docker-compose.gpu.yml (резервирование видеокарт).
# Без NVIDIA стек поднимается на CPU — тот же файл, те же команды.
# ============================================================

# --- Автоопределение GPU -------------------------------------------
# nvidia-smi -L печатает список карт; пустой вывод = GPU нет
GPU := $(shell nvidia-smi -L 2>/dev/null)

ifeq ($(GPU),)
COMPOSE_FILES = -f docker-compose.yml
else
COMPOSE_FILES = -f docker-compose.yml -f docker-compose.gpu.yml
endif

.PHONY: help up down restart logs ps build migrate test lint clean

help: ## Показать список команд
	@echo "Vision Analytics Platform — команды:"
	@echo ""
	@echo "  make up       поднять весь стек (GPU детектится автоматически)"
	@echo "  make down     остановить стек"
	@echo "  make restart  перезапустить стек"
	@echo "  make logs     логи всех сервисов (stream)"
	@echo "  make ps       статус сервисов"
	@echo "  make build    пересобрать образы"
	@echo "  make migrate  применить миграции Alembic"
	@echo "  make test     прогнать тесты (нужен локальный venv)"
	@echo "  make lint     ruff-проверка (нужен локальный venv)"
	@echo "  make clean    остановить и удалить контейнеры/тома (ОСТОРОЖНО: данные)"
	@echo ""
	@echo "GPU: $(if $(GPU),найден → $(GPU),не найден → запуск на CPU)"

up: ## Поднять все сервисы (с GPU-override, если есть NVIDIA)
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
	ruff check app dashboard tests

clean: ## Остановить и удалить контейнеры и тома (ДАННЫЕ БД/MinIO будут потеряны)
	docker compose $(COMPOSE_FILES) down -v
