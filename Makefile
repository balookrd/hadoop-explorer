.PHONY: help venv sync install-dev lint format test test-hdfs test-spark test-sql test-yarn \
        build build-hdfs build-spark build-sql build-yarn \
        frontend-build frontend-install generate-types demo-hdfs demo-spark demo-sql demo-yarn demo-all \
        demo-hdfs-stop demo-spark-stop demo-sql-stop demo-yarn-stop demo-all-stop helm-lint helm-package

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
	@echo "    make test             - Запуск всех 127 модульных тестов платформы"
	@echo "    make test-hdfs        - Тесты сервиса HDFS Explorer (40 тестов)"
	@echo "    make test-spark       - Тесты сервиса Spark Explorer (13 тестов)"
	@echo "    make test-sql         - Тесты сервиса SQL Explorer (32 теста)"
	@echo "    make test-yarn        - Тесты сервиса YARN Explorer (42 теста)"
	@echo ""
	@echo "  Сборка Docker-контейнеров:"
	@echo "    make build            - Сборка всех Docker-образов (hdfs, spark, sql, yarn)"
	@echo "    make build-hdfs       - Сборка образа HDFS Explorer"
	@echo "    make build-spark      - Сборка образа Spark Explorer"
	@echo "    make build-sql        - Сборка образа SQL Explorer"
	@echo "    make build-yarn       - Сборка образа YARN Explorer"
	@echo ""
	@echo "  Фронтенд:"
	@echo "    make frontend-install - Установка зависимостей frontend apps"
	@echo "    make frontend-build   - Сборка всех SPA приложений"
	@echo "    make generate-types   - Генерация TypeScript типов из OpenAPI схем FastAPI бэкенда"
	@echo ""
	@echo "  Раздельные демо стенды (Docker Compose):"
	@echo "    make demo-hdfs        - Запуск стенда HDFS (WebHDFS, Kerberos, OpenLDAP)"
	@echo "    make demo-hdfs-stop   - Остановка стенда HDFS"
	@echo "    make demo-spark       - Запуск стенда Spark (Livy, PySpark, Scala, Metastore)"
	@echo "    make demo-spark-stop  - Остановка стенда Spark"
	@echo "    make demo-sql         - Запуск стенда SQL (Trino, Hive, Postgres, LDAP)"
	@echo "    make demo-sql-stop    - Остановка стенда SQL"
	@echo "    make demo-yarn        - Запуск стенда YARN (2 RM кластера, Kerberos, LDAP)"
	@echo "    make demo-yarn-stop   - Остановка стенда YARN"
	@echo "    make demo-all         - Запуск объединенного демо-стенда"
	@echo "    make demo-all-stop    - Остановка объединенного демо-стенда"
	@echo ""
	@echo "  Kubernetes / Helm:"
	@echo "    make helm-lint        - Проверка синтаксиса всех Helm-чартов"
	@echo "    make helm-package     - Упаковка чартов для деплоя"
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

test-hdfs:
	./scripts/run-tests.sh hdfs

test-spark:
	./scripts/run-tests.sh spark

test-sql:
	./scripts/run-tests.sh sql

test-yarn:
	./scripts/run-tests.sh yarn

build:
	TAG=$(TAG) REGISTRY=$(REGISTRY) ./scripts/build-containers.sh all

build-hdfs:
	TAG=$(TAG) REGISTRY=$(REGISTRY) ./scripts/build-containers.sh hdfs

build-spark:
	TAG=$(TAG) REGISTRY=$(REGISTRY) ./scripts/build-containers.sh spark

build-sql:
	TAG=$(TAG) REGISTRY=$(REGISTRY) ./scripts/build-containers.sh sql

build-yarn:
	TAG=$(TAG) REGISTRY=$(REGISTRY) ./scripts/build-containers.sh yarn

frontend-install:
	cd frontend && npm install

frontend-build:
	cd frontend && npm run build:all

generate-types:
	./scripts/generate-types.sh

demo-platform:
	cd demo/platform && ./start-platform.sh

demo-platform-stop:
	cd demo/platform && ./stop-platform.sh

demo-hdfs:
	cd demo/hdfs && ./start-demo.sh

demo-hdfs-stop:
	cd demo/hdfs && ./stop-demo.sh

demo-spark:
	cd demo/spark && ./start-demo.sh

demo-spark-stop:
	cd demo/spark && ./stop-demo.sh

demo-sql:
	cd demo/sql && ./start-demo.sh

demo-sql-stop:
	cd demo/sql && ./stop-demo.sh

demo-yarn:
	cd demo/yarn && ./start-demo.sh

demo-yarn-stop:
	cd demo/yarn && ./stop-demo.sh

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
