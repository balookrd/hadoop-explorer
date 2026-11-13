.PHONY: help test test-artel test-sklad test-sql test-spark test-replicator \
        test-zastava test-replicator-agent test-replicator-orchestrator test-java test-ui \
        build build-java build-zastava build-artel build-sklad build-sql build-spark build-replicator \
        frontend-build frontend-install demo-artel demo-sklad demo-sql demo-spark demo-all \
        demo-artel-stop demo-sklad-stop demo-sql-stop demo-spark-stop demo-all-stop \
        skeleton skeleton-all skeleton-backend java-index java-query check-jdk21

TAG ?= latest
REGISTRY ?= pristan

# Платформа проекта и CI — Java 21 LTS. Прогоны на JDK 24 и новее бессмысленны: Hadoop 3.3.6
# теряет имперсонацию (`Subject.getSubject` бросает UnsupportedOperationException после удаления
# Security Manager, JEP 486), отключить это флагом нельзя — `-Djava.security.manager=allow` не
# дает стартовать VM. Поэтому сюита там зеленеет, ничего не проверяя в части Kerberos.
#
# JAVA21_HOME ищется в таком порядке: текущий JAVA_HOME, если это 21; java_home -v 21 (macOS);
# каталоги JDK с «21» в имени (установка из IDE, Linux). Можно задать явно:
#   make test-replicator JAVA21_HOME=/path/to/jdk-21
JAVA21_HOME ?= $(shell \
	if [ -x "$$JAVA_HOME/bin/java" ] && "$$JAVA_HOME/bin/java" -version 2>&1 | grep -q 'version "21'; then \
		echo "$$JAVA_HOME"; \
	elif /usr/libexec/java_home -v 21 >/dev/null 2>&1; then \
		/usr/libexec/java_home -v 21; \
	else \
		ls -d $$HOME/Library/Java/JavaVirtualMachines/*21* /usr/lib/jvm/*21* 2>/dev/null | head -1; \
	fi)

JAVA21 := JAVA_HOME=$(JAVA21_HOME)

check-jdk21:
	@if [ -z "$(JAVA21_HOME)" ] || [ ! -x "$(JAVA21_HOME)/bin/java" ]; then \
		echo "JDK 21 не найден. Задайте путь явно: make <цель> JAVA21_HOME=/path/to/jdk-21"; \
		exit 1; \
	fi
	@if ! "$(JAVA21_HOME)/bin/java" -version 2>&1 | grep -q 'version "21'; then \
		echo "JAVA21_HOME указывает не на Java 21: $(JAVA21_HOME)"; \
		"$(JAVA21_HOME)/bin/java" -version; \
		exit 1; \
	fi
	@echo "Java 21: $(JAVA21_HOME)"

help:
	@echo "========================================================================"
	@echo "                              PRISTAN CLI                               "
	@echo "========================================================================"
	@echo "  Тестирование (Java 21 / Spring Boot 3 + UI):"
	@echo "    make test             - Запуск всех модульных тестов платформы (Java + UI)"
	@echo "    make test-artel       - Тесты сервиса Artel (Java 21)"
	@echo "    make test-sklad        - Тесты сервиса Sklad (Java 21)"
	@echo "    make test-sql         - Тесты сервиса SQL Explorer (Java 21)"
	@echo "    make test-spark       - Тесты сервиса Spark Explorer (Java 21)"
	@echo "    make test-replicator  - Тесты сервиса Replicator (Java 21)"
	@echo "    make test-zastava - Тесты Java 21 zastava"
	@echo "    make test-java        - Запуск всех Java тестов платформы"
	@echo "    make test-ui          - Запуск тестов фронтенда (Vitest / Svelte 5)"
	@echo "    make check-jdk21      - Проверка, что найден JDK 21 (платформа проекта и CI)"
	@echo ""
	@echo "  Сборка JAR-пакетов и Docker-контейнеров:"
	@echo "    make build-java       - Сборка всех Java JAR-пакетов платформы (Maven Reactor)"
	@echo "    make build-zastava - Сборка zastava JAR"
	@echo "    make build            - Сборка всех Docker-образов (yarn, hdfs, sql, spark, replicator)"
	@echo "    make build-artel      - Сборка образа Artel"
	@echo "    make build-sklad       - Сборка образа Sklad"
	@echo "    make build-sql        - Сборка образа SQL Explorer"
	@echo "    make build-spark      - Сборка образа Spark Explorer"
	@echo "    make build-replicator - Сборка образа Replicator Orchestrator"
	@echo ""
	@echo "  Фронтенд:"
	@echo "    make frontend-install - Установка зависимостей frontend apps"
	@echo "    make frontend-build   - Сборка всех SPA приложений"
	@echo ""
	@echo "  Раздельные демо стенды (Docker Compose):"
	@echo "    make demo-artel       - Запуск стенда YARN (2 RM кластера, Kerberos, LDAP) -> :8001"
	@echo "    make demo-artel-stop  - Остановка стенда YARN"
	@echo "    make demo-sklad        - Запуск стенда HDFS (WebHDFS, Kerberos, OpenLDAP) -> :8002"
	@echo "    make demo-sklad-stop   - Остановка стенда HDFS"
	@echo "    make demo-sql         - Запуск стенда SQL (Trino, Hive, Postgres, LDAP) -> :8003"
	@echo "    make demo-sql-stop    - Остановка стенда SQL"
	@echo "    make demo-spark       - Запуск стенда Spark (Livy, PySpark, Scala, Metastore) -> :8004"
	@echo "    make demo-spark-stop  - Остановка стенда Spark"
	@echo "    make demo-monitoring  - Запуск стека мониторинга (Prometheus :9090 + Grafana :3000)"
	@echo "    make demo-monitoring-stop - Остановка стека мониторинга"
	@echo "    make demo-all         - Запуск объединенного демо-стенда (все сервисы + мониторинг)"
	@echo "    make demo-all-stop    - Остановка объединенного демо-стенда"
	@echo ""
	@echo "  AST-скелетизация, индексация и контекст для LLM:"
	@echo "    make java-index       - Генерация AST-индекса Java (target/java-ast-index/)"
	@echo "    make java-query Q=\"...\" - Быстрый поиск по Java коду (например: make java-query Q=\"class Auth\")"
	@echo "========================================================================"

test:
	./scripts/run-tests.sh all

test-artel: check-jdk21
	$(JAVA21) mvn test -f backend/artel/pom.xml

test-sklad: check-jdk21
	$(JAVA21) mvn test -f backend/sklad/pom.xml

test-sql: check-jdk21
	$(JAVA21) mvn test -f backend/sql/pom.xml

test-spark: check-jdk21
	$(JAVA21) mvn test -f backend/spark/pom.xml

test-replicator: check-jdk21
	$(JAVA21) mvn test -f backend/replicator/pom.xml

test-replicator-agent: check-jdk21
	$(JAVA21) mvn test -f backend/replicator/pom.xml -pl agent -am

test-replicator-orchestrator: check-jdk21
	$(JAVA21) mvn test -f backend/replicator/pom.xml -pl orchestrator -am

test-replicator-agent-java: test-replicator-agent
test-replicator-orchestrator-java: test-replicator-orchestrator
test-replicator-java: test-replicator

test-zastava: check-jdk21
	$(JAVA21) mvn test -f backend/zastava/pom.xml

test-sklad-java: test-sklad
test-artel-java: test-artel
test-sql-java: test-sql
test-spark-java: test-spark

test-java: test-zastava test-replicator test-sklad test-artel test-sql test-spark

test-ui:
	./scripts/run-tests.sh frontend

build:
	TAG=$(TAG) REGISTRY=$(REGISTRY) ./scripts/build-containers.sh all

build-zastava: check-jdk21
	$(JAVA21) mvn clean package -DskipTests -f backend/zastava/pom.xml

build-java: check-jdk21 frontend-build
	$(JAVA21) mvn clean package -DskipTests

build-artel: frontend-build-artel
	$(JAVA21) mvn clean package -DskipTests -f backend/artel/pom.xml

build-sklad: frontend-build-sklad
	$(JAVA21) mvn clean package -DskipTests -f backend/sklad/pom.xml

build-sql: frontend-build-sql
	$(JAVA21) mvn clean package -DskipTests -f backend/sql/pom.xml

build-spark: frontend-build-spark
	$(JAVA21) mvn clean package -DskipTests -f backend/spark/pom.xml

build-replicator: frontend-build-replicator
	$(JAVA21) mvn clean package -DskipTests -f backend/replicator/pom.xml

build-replicator-agent:
	$(JAVA21) mvn clean package -DskipTests -f backend/replicator/pom.xml -pl agent -am

build-replicator-orchestrator: frontend-build-replicator
	$(JAVA21) mvn clean package -DskipTests -f backend/replicator/pom.xml -pl orchestrator -am

build-replicator-agent-java: build-replicator-agent
build-replicator-orchestrator-java: build-replicator-orchestrator
build-replicator-java: build-replicator

build-sklad-java: build-sklad
build-artel-java: build-artel
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

# UI собирается прямо в backend/*/src/main/resources/static и в git не хранится, поэтому
# сборка JAR сначала собирает UI своего сервиса
frontend/node_modules: frontend/package-lock.json
	cd frontend && npm ci
	@touch frontend/node_modules

frontend-build: frontend/node_modules
	cd frontend && npm run build:all

frontend-build-%: frontend/node_modules
	cd frontend && npm run build:$*

generate-proto:
	$(JAVA21) mvn compile -DskipTests -f backend/replicator/agent/pom.xml

proto: generate-proto

demo-platform:
	cd demo/platform && ./start-platform.sh

demo-platform-stop:
	cd demo/platform && ./stop-platform.sh

demo-artel:
	cd demo/artel && ./start-demo.sh

demo-artel-stop:
	cd demo/artel && ./stop-demo.sh

demo-sklad:
	cd demo/sklad && ./start-demo.sh

demo-sklad-stop:
	cd demo/sklad && ./stop-demo.sh

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

demo-monitoring-smoke:
	cd demo/monitoring && ./run-monitoring-smoke-tests.sh

demo-kafka-smoke:
	cd demo/replicator && ./run-kafka-smoke-tests.sh

demo-all:
	cd demo/all && ./start-all-demo.sh

demo-all-stop:
	cd demo/all && ./stop-all-demo.sh

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

