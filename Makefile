.PHONY: help test test-yarn test-hdfs test-sql test-spark test-replicator \
        test-security-starter test-replicator-agent test-replicator-orchestrator test-java test-ui \
        build build-yarn build-hdfs build-sql build-spark build-replicator \
        frontend-build frontend-install demo-yarn demo-hdfs demo-sql demo-spark demo-all \
        demo-yarn-stop demo-hdfs-stop demo-sql-stop demo-spark-stop demo-all-stop helm-lint helm-package \
        skeleton skeleton-all skeleton-backend java-index java-query

TAG ?= latest
REGISTRY ?= hadoop-explorer

help:
	@echo "========================================================================"
	@echo "                   HADOOP EXPLORER PLATFORM CLI                         "
	@echo "========================================================================"
	@echo "  Тестирование (Java 21 / Spring Boot 3 + UI):"
	@echo "    make test             - Запуск всех модульных тестов платформы (Java + UI)"
	@echo "    make test-yarn        - Тесты сервиса YARN Explorer (Java 21)"
	@echo "    make test-hdfs        - Тесты сервиса HDFS Explorer (Java 21)"
	@echo "    make test-sql         - Тесты сервиса SQL Explorer (Java 21)"
	@echo "    make test-spark       - Тесты сервиса Spark Explorer (Java 21)"
	@echo "    make test-replicator  - Тесты сервиса Replicator (Java 21)"
	@echo "    make test-security-starter - Тесты Java 21 common-security-starter"
	@echo "    make test-java        - Запуск всех Java тестов платформы"
	@echo "    make test-ui          - Запуск тестов фронтенда (Vitest / Svelte 5)"
	@echo ""
	@echo "  Сборка Docker-контейнеров:"
	@echo "    make build            - Сборка всех Docker-образов (yarn, hdfs, sql, spark, replicator)"
	@echo "    make build-yarn       - Сборка образа YARN Explorer"
	@echo "    make build-hdfs       - Сборка образа HDFS Explorer"
	@echo "    make build-sql        - Сборка образа SQL Explorer"
	@echo "    make build-spark      - Сборка образа Spark Explorer"
	@echo "    make build-replicator - Сборка образа Replicator Orchestrator"
	@echo ""
	@echo "  Фронтенд:"
	@echo "    make frontend-install - Установка зависимостей frontend apps"
	@echo "    make frontend-build   - Сборка всех SPA приложений"
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
	@echo "  AST-скелетизация, индексация и контекст для LLM:"
	@echo "    make java-index       - Генерация AST-индекса Java (target/java-ast-index/)"
	@echo "    make java-query Q=\"...\" - Быстрый поиск по Java коду (например: make java-query Q=\"class Auth\")"
	@echo "========================================================================"

test:
	./scripts/run-tests.sh all

test-yarn:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn test -f backend/yarn/pom.xml

test-hdfs:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn test -f backend/hdfs/pom.xml

test-sql:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn test -f backend/sql/pom.xml

test-spark:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn test -f backend/spark/pom.xml

test-replicator:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn test -f backend/replicator/pom.xml

test-replicator-agent:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn test -f backend/replicator/agent/pom.xml

test-replicator-orchestrator:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn test -f backend/replicator/orchestrator/pom.xml

test-replicator-agent-java: test-replicator-agent
test-replicator-orchestrator-java: test-replicator-orchestrator
test-replicator-java: test-replicator

test-security-starter:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn test -f backend/common-security-starter/pom.xml

test-hdfs-java: test-hdfs
test-yarn-java: test-yarn
test-sql-java: test-sql
test-spark-java: test-spark

test-java: test-security-starter test-replicator test-hdfs test-yarn test-sql test-spark

test-ui:
	./scripts/run-tests.sh frontend

build:
	TAG=$(TAG) REGISTRY=$(REGISTRY) ./scripts/build-containers.sh all

build-yarn:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn clean package -DskipTests -f backend/yarn/pom.xml

build-hdfs:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn clean package -DskipTests -f backend/hdfs/pom.xml

build-sql:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn clean package -DskipTests -f backend/sql/pom.xml

build-spark:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn clean package -DskipTests -f backend/spark/pom.xml

build-replicator:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn clean package -DskipTests -f backend/replicator/pom.xml

build-replicator-agent:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn clean package -DskipTests -f backend/replicator/agent/pom.xml

build-replicator-orchestrator:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn clean package -DskipTests -f backend/replicator/orchestrator/pom.xml

build-replicator-agent-java: build-replicator-agent
build-replicator-orchestrator-java: build-replicator-orchestrator
build-replicator-java: build-replicator

build-hdfs-java: build-hdfs
build-yarn-java: build-yarn
build-sql-java: build-sql
build-spark-java: build-spark

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

generate-proto:
	JAVA_HOME=$${JAVA_HOME:-/opt/homebrew/opt/openjdk} mvn compile -DskipTests -f backend/replicator/agent/pom.xml

proto: generate-proto

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

demo-replicator:
	cd demo/replicator && ./start-demo.sh

demo-replicator-stop:
	cd demo/replicator && ./stop-demo.sh

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
	python3 scripts/generate_skeleton.py frontend/common/types frontend/common/api --output .context/skeleton.md

skeleton-all: java-index
	mkdir -p .context
	python3 scripts/generate_skeleton.py frontend/common frontend/apps --output .context/all_skeleton.md

java-index:
	./scripts/build-java-ast-index.sh

java-query:
	./scripts/java-index-query.sh $(Q)

