.PHONY: help venv sync install-dev lint format test test-yarn test-hdfs test-sql test-spark \
        build build-yarn build-hdfs build-sql build-spark \
        frontend-build frontend-install generate-types demo-yarn demo-hdfs demo-sql demo-spark demo-all \
        demo-yarn-stop demo-hdfs-stop demo-sql-stop demo-spark-stop demo-all-stop helm-lint helm-package \
        skeleton skeleton-all skeleton-backend

TAG ?= latest
REGISTRY ?= hadoop-explorer

help:
	@echo "========================================================================"
	@echo "                   HADOOP EXPLORER PLATFORM CLI                         "
	@echo "========================================================================"
	@echo "  Окружение Python (uv workspaces):"
	@echo "    make venv / make sync - Синхронизация единого uv-окружения (.venv)"
	@echo "    make install-dev      - Установка всех пакетов и dev-зависимостей"
	@echo "    make lint             - Проверка линтером Ruff"
	@echo "    make format           - Автоформатирование кода с помощью Ruff"
	@echo ""
	@echo "  Тестирование:"
	@echo "    make test             - Запуск всех модульных тестов платформы"
	@echo "    make test-yarn        - Тесты сервиса YARN Explorer"
	@echo "    make test-hdfs        - Тесты сервиса HDFS Explorer"
	@echo "    make test-sql         - Тесты сервиса SQL Explorer"
	@echo "    make test-spark       - Тесты сервиса Spark Explorer"
	@echo ""
	@echo "  Сборка Docker-контейнеров:"
	@echo "    make build            - Сборка всех Docker-образов (yarn, hdfs, sql, spark)"
	@echo "    make build-yarn       - Сборка образа YARN Explorer"
	@echo "    make build-hdfs       - Сборка образа HDFS Explorer"
	@echo "    make build-sql        - Сборка образа SQL Explorer"
	@echo "    make build-spark      - Сборка образа Spark Explorer"
	@echo ""
	@echo "  Фронтенд:"
	@echo "    make frontend-install - Установка зависимостей frontend apps"
	@echo "    make frontend-build   - Сборка всех SPA приложений"
	@echo "    make generate-types   - Генерация TypeScript типов из OpenAPI схем FastAPI бэкенда"
	@echo ""
	@echo "  Раздельные демо стенды (Docker Compose):"
	@echo "    make demo-yarn        - Запуск стенда YARN (2 RM кластера, Kerberos, LDAP) -> :8001"
	@echo "    make demo-yarn-stop   - Остановка стенда YARN"
	@echo "    make demo-hdfs        - Запуск стенда HDFS (WebHDFS, Kerberos, OpenLDAP) -> :8002"
	@echo "    make demo-hdfs-stop   - Остановка стенда HDFS"
	@echo "    make demo-sql         - Запуск стенда SQL (Trino, Hive, Postgres, LDAP) -> :8003"
	@echo "    make demo-sql-stop    - Остановка стенда SQL"
	@echo "    make demo-spark       - Запуск стенда Spark (Livy, PySpark, Scala, Metastore) -> :8004"
	@echo "    make demo-spark-stop  - Остановка стенда Spark"
	@echo "    make demo-monitoring  - Запуск стека мониторинга (Prometheus :9090 + Grafana :3000)"
	@echo "    make demo-monitoring-stop - Остановка стека мониторинга"
	@echo "    make demo-all         - Запуск объединенного демо-стенда (все сервисы + мониторинг)"
	@echo "    make demo-all-stop    - Остановка объединенного демо-стенда"
	@echo ""
	@echo "  Kubernetes / Helm:"
	@echo "    make helm-lint        - Проверка синтаксиса всех Helm-чартов"
	@echo "    make helm-package     - Упаковка чартов для деплоя"
	@echo ""
	@echo "  AST-скелетизация и контекст для LLM:"
	@echo "    make skeleton         - Генерация легковесного AST-скелета API и контрактов"
	@echo "    make skeleton-backend - Генерация AST-скелета только для бэкенда"
	@echo "    make skeleton-all     - Генерация полного AST-каркаса платформы"
	@echo "========================================================================"

venv:
	uv sync --all-packages

sync:
	uv sync --all-packages

install-dev:
	uv sync --all-packages

lint:
	uv run ruff check backend

format:
	uv run ruff format backend

test:
	./scripts/run-tests.sh all

test-yarn:
	./scripts/run-tests.sh yarn

test-hdfs:
	./scripts/run-tests.sh hdfs

test-sql:
	./scripts/run-tests.sh sql

test-spark:
	./scripts/run-tests.sh spark

test-ui:
	./scripts/run-tests.sh frontend

build:
	TAG=$(TAG) REGISTRY=$(REGISTRY) ./scripts/build-containers.sh all

build-yarn:
	TAG=$(TAG) REGISTRY=$(REGISTRY) ./scripts/build-containers.sh yarn

build-hdfs:
	TAG=$(TAG) REGISTRY=$(REGISTRY) ./scripts/build-containers.sh hdfs

build-sql:
	TAG=$(TAG) REGISTRY=$(REGISTRY) ./scripts/build-containers.sh sql

build-spark:
	TAG=$(TAG) REGISTRY=$(REGISTRY) ./scripts/build-containers.sh spark

frontend-install:
	cd frontend && npm install

frontend-check:
	cd frontend && npm run check:all

frontend-test:
	cd frontend && npm test

frontend-e2e:
	cd frontend && npm run test:e2e

frontend-build:
	cd frontend && npm run build:all

generate-types:
	./scripts/generate-types.sh

demo-platform:
	cd demo/platform && ./start-platform.sh

demo-platform-stop:
	cd demo/platform && ./stop-platform.sh

demo-yarn:
	cd demo/yarn && ./start-demo.sh

demo-yarn-stop:
	cd demo/yarn && ./stop-demo.sh

demo-hdfs:
	cd demo/hdfs && ./start-demo.sh

demo-hdfs-stop:
	cd demo/hdfs && ./stop-demo.sh

demo-sql:
	cd demo/sql && ./start-demo.sh

demo-sql-stop:
	cd demo/sql && ./stop-demo.sh

demo-spark:
	cd demo/spark && ./start-demo.sh

demo-spark-stop:
	cd demo/spark && ./stop-demo.sh

demo-monitoring:
	cd demo/monitoring && ./start-monitoring.sh

demo-monitoring-stop:
	cd demo/monitoring && ./stop-monitoring.sh

demo-all:
	cd demo/all && ./start-all-demo.sh

demo-all-stop:
	cd demo/all && ./stop-all-demo.sh

helm-lint:
	helm lint helm/charts/hdfs-explorer
	helm lint helm/charts/spark-explorer
	helm lint helm/charts/sql-explorer
	helm lint helm/charts/yarn-explorer
	helm lint helm/hadoop-explorer

helm-package: helm-lint
	helm package helm/charts/hdfs-explorer -d dist/helm
	helm package helm/charts/spark-explorer -d dist/helm
	helm package helm/charts/sql-explorer -d dist/helm
	helm package helm/charts/yarn-explorer -d dist/helm
	helm package helm/hadoop-explorer -d dist/helm

skeleton:
	mkdir -p .context
	python3 scripts/generate_skeleton.py backend/common backend/yarn backend/hdfs backend/sql backend/spark frontend/common/types frontend/common/api --output .context/skeleton.md

skeleton-backend:
	mkdir -p .context
	python3 scripts/generate_skeleton.py backend --output .context/backend_skeleton.md

skeleton-all:
	mkdir -p .context
	python3 scripts/generate_skeleton.py backend frontend/common frontend/apps --output .context/all_skeleton.md

