from __future__ import annotations
import difflib
import json
import logging
import re
import time
from typing import Dict, Any, List, Optional, Set
import httpx
from pydantic import BaseModel, Field
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from app.core.config import AIConfig, settings
from app.services.mock_engine import MOCK_CATALOGS

logger = logging.getLogger("ai_service")

# --- Модели данных для API ответов ---


class AIIssue(BaseModel):
    line: int = Field(default=1, description="Номер строки (1-based)")
    column: int = Field(default=1, description="Номер символа в строке (1-based)")
    end_line: Optional[int] = Field(default=None, description="Конечная строка")
    end_column: Optional[int] = Field(default=None, description="Конечный символ")
    severity: str = Field(default="warning", description="Уровень: 'error', 'warning', 'info'")
    category: str = Field(
        default="performance", description="Категория: 'syntax', 'performance', 'schema', 'security', 'dialect'"
    )
    message: str = Field(..., description="Описание проблемы на русском языке")
    rule: str = Field(..., description="Идентификатор правила проверки")
    suggestion: Optional[str] = Field(default=None, description="Рекомендация по исправлению")


class AICheckResponse(BaseModel):
    is_valid: bool
    issues: List[AIIssue]
    summary: str
    complexity_score: int = 1  # 1-10
    complexity_level: str = "Низкая"  # Низкая, Средняя, Высокая, Критическая
    estimated_notes: List[str] = []
    model: str
    provider: str
    execution_time_ms: float = 0.0
    fallback_used: bool = False


class AIExplainResponse(BaseModel):
    explanation: str
    summary: str
    tables_used: List[str] = []
    operations: List[str] = []
    model: str
    provider: str
    execution_time_ms: float = 0.0


class AIOptimizeResponse(BaseModel):
    original_sql: str
    optimized_sql: str
    optimizations: List[str]
    diff_summary: str
    model: str
    provider: str
    execution_time_ms: float = 0.0


class AIFixResponse(BaseModel):
    original_sql: str
    fixed_sql: str
    explanation: str
    model: str
    provider: str
    execution_time_ms: float = 0.0


class AIGenerateResponse(BaseModel):
    prompt: str = Field(..., description="Исходный запрос на естественном языке")
    generated_sql: str = Field(..., description="Сгенерированный SQL-запрос")
    explanation: str = Field(..., description="Пояснение к сгенерированному запросу")
    tables_used: List[str] = Field(default_factory=list, description="Список задействованных таблиц")
    model: str
    provider: str
    execution_time_ms: float = 0.0
    fallback_used: bool = False


class AIFormatResponse(BaseModel):
    original_sql: str
    formatted_sql: str
    dialect: str


class AIStatusResponse(BaseModel):
    enabled: bool
    provider: str
    model: str
    base_url: str
    available: bool
    message: str
    latency_ms: Optional[float] = None


# --- Функции безопасности и защиты от Prompt/SQL Injection ---

MAX_SQL_INPUT_CHARS = 50000
DISALLOWED_AST_NODES = tuple(
    [
        node
        for node in [
            getattr(exp, "Insert", None),
            getattr(exp, "Update", None),
            getattr(exp, "Delete", None),
            getattr(exp, "Drop", None),
            getattr(exp, "Alter", None),
            getattr(exp, "Create", None),
            getattr(exp, "Truncate", None),
            getattr(exp, "Command", None),
            getattr(exp, "Merge", None),
            getattr(exp, "Grant", None),
        ]
        if node is not None
    ]
)


def sanitize_prompt_input(text: str, max_chars: int = MAX_SQL_INPUT_CHARS) -> str:
    """
    Санитизирует пользовательский ввод перед отправкой в LLM:
    - Ограничивает размер строки (защита от DoS / Token Exhaustion).
    - Удаляет управляющие теги и попытки Prompt Injection (System Prompt Override).
    """
    if not text:
        return ""
    cleaned = str(text).strip()[:max_chars]
    cleaned = re.sub(r"(?i)<\s*(system|instruction|sys)\s*>", "", cleaned)
    cleaned = re.sub(r"(?i)<\s*/\s*(system|instruction|sys)\s*>", "", cleaned)
    cleaned = re.sub(r"(?i)\[\s*(system|instruction)\s*\]", "", cleaned)
    return cleaned


def validate_readonly_sql_ast(sql: str, dialect: str = "trino") -> tuple[bool, Optional[str]]:
    """
    Проверяет, что SQL-запрос является строго Read-Only на уровне синтаксического дерева (AST) sqlglot.
    Возвращает (is_safe, error_message).
    """
    sql_stripped = sql.strip()
    if not sql_stripped:
        return True, None

    # Быстрая проверка начальной команды
    clean_no_comments = re.sub(r"--.*?\n|/\*.*?\*/", " ", sql_stripped, flags=re.DOTALL)
    words = clean_no_comments.strip().split()
    if words:
        first_word = words[0].upper()
        if first_word in (
            "DROP",
            "TRUNCATE",
            "DELETE",
            "INSERT",
            "UPDATE",
            "ALTER",
            "CREATE",
            "MERGE",
            "GRANT",
            "REVOKE",
        ):
            return (
                False,
                f"Команда {first_word} запрещена. Разрешены только аналитические запросы на чтение данных (SELECT / WITH / EXPLAIN / SHOW / DESCRIBE).",
            )

    sql_dialect = MockSQLAnalyzer._map_dialect(dialect)
    try:
        trees = sqlglot.parse(sql_stripped, read=sql_dialect)
        for tree in trees:
            if not tree:
                continue
            for disallowed_cls in DISALLOWED_AST_NODES:
                if isinstance(tree, disallowed_cls) or tree.find(disallowed_cls):
                    node_name = disallowed_cls.__name__.replace("Table", "").upper()
                    return (
                        False,
                        f"Команда {node_name} запрещена. Разрешены только аналитические запросы на чтение данных (SELECT / WITH / EXPLAIN / SHOW / DESCRIBE).",
                    )
        return True, None
    except Exception as e:
        logger.warning(f"Ошибка AST-парсинга SQL-запроса ({sql_stripped[:100]}...): {e}")
        return (
            False,
            f"Не удалось верифицировать безопасность SQL-запроса: синтаксическая ошибка или неподдерживаемый диалект ({e})",
        )


# --- Промышленный AST-анализатор SQL (sqlglot AST Linter & Optimizer) ---


class MockSQLAnalyzer:
    """
    Высокопроизводительный статический анализатор SQL на базе AST sqlglot.
    Поддерживает точный разбор синтаксических деревьев, проверку диалектов Trino/Hive,
    валидацию схемы с fuzzy-подсказками и расчет индекса сложности запроса.
    """

    @staticmethod
    def _map_dialect(dialect: str) -> str:
        d = (dialect or "trino").lower()
        if "hive" in d:
            return "hive"
        if "postgres" in d:
            return "postgres"
        return "trino"

    @classmethod
    def check(
        cls, sql: str, dialect: str = "trino", catalog_context: Optional[Dict[str, Any]] = None
    ) -> AICheckResponse:
        issues: List[AIIssue] = []
        sql_dialect = cls._map_dialect(dialect)
        sql_stripped = sql.strip()

        if not sql_stripped:
            return AICheckResponse(
                is_valid=True,
                issues=[],
                summary="Запрос пуст.",
                complexity_score=1,
                complexity_level="Низкая",
                estimated_notes=[],
                model="ast-sqlglot-analyzer",
                provider="mock",
                fallback_used=True,
            )

        # 1. AST Парсинг синтаксиса через sqlglot
        parsed_trees: List[exp.Expression] = []
        try:
            parsed_trees = sqlglot.parse(sql_stripped, read=sql_dialect)
        except ParseError as pe:
            for error in pe.errors:
                issues.append(
                    AIIssue(
                        line=error.get("line") or 1,
                        column=error.get("col") or 1,
                        severity="error",
                        category="syntax",
                        message=f"Синтаксическая ошибка ({sql_dialect.upper()}): {error.get('description', 'Некорректная конструкция')}",
                        rule="syntax-error",
                        suggestion="Проверьте правильность расстановки запятых, скобок и ключевых слов SQL.",
                    )
                )
        except Exception as e:
            issues.append(
                AIIssue(
                    line=1,
                    column=1,
                    severity="error",
                    category="syntax",
                    message=f"Ошибка синтаксического анализа: {str(e)}",
                    rule="syntax-error",
                    suggestion="Проверьте синтаксис запроса.",
                )
            )

        # 2. Если парсер упал, возвращаем синтаксические ошибки
        if not parsed_trees and issues:
            return AICheckResponse(
                is_valid=False,
                issues=issues,
                summary=f"Обнаружены синтаксические ошибки ({len(issues)}).",
                complexity_score=1,
                complexity_level="Низкая",
                estimated_notes=["Исправьте синтаксические ошибки для выполнения запроса."],
                model="ast-sqlglot-analyzer",
                provider="mock",
                fallback_used=True,
            )

        # 3. Анализ каждого выражения в AST
        complexity_points = 1.0
        complexity_notes = []

        for tree in parsed_trees:
            if not tree:
                continue

            # A. Проверка деструктивных операций (Security)
            if isinstance(tree, (exp.Drop, exp.TruncateTable)):
                issues.append(
                    AIIssue(
                        line=1,
                        column=1,
                        severity="error",
                        category="security",
                        message="Обнаружена деструктивная DDL-команда (DROP/TRUNCATE).",
                        rule="destructive-ddl",
                        suggestion="Убедитесь, что удаление структуры или очистка таблицы согласованы с владельцем данных.",
                    )
                )
                complexity_points += 4.0
                complexity_notes.append("Деструктивная операция изменения схемы/удаления")

            if isinstance(tree, exp.Delete):
                issues.append(
                    AIIssue(
                        line=1,
                        column=1,
                        severity="warning",
                        category="security",
                        message="Команда DELETE в аналитических движках (Trino/Hive) ресурсоемка и требует поддержки ACID/Iceberg/Delta.",
                        rule="destructive-dml",
                        suggestion="Используйте партиционное удаление или перезапись витрины.",
                    )
                )

            # B. Проверка SELECT * (Star)
            for select_node in tree.find_all(exp.Select):
                has_star = any(isinstance(expr, exp.Star) for expr in select_node.expressions)
                if has_star:
                    issues.append(
                        AIIssue(
                            line=1,
                            column=1,
                            severity="warning",
                            category="performance",
                            message="Использование 'SELECT *' не рекомендуется в колоночных аналитических хранилищах (Parquet/ORC).",
                            rule="select-star",
                            suggestion="Явно перечислите только необходимые столбцы для колоночного чтения и уменьшения I/O.",
                        )
                    )
                    complexity_points += 1.0

            # C. Проверка отсутствия LIMIT в основном запросе
            if isinstance(tree, exp.Select):
                if not tree.args.get("limit"):
                    # Проверяем, есть ли агрегации без GROUP BY (например SELECT COUNT(*))
                    has_group = bool(tree.args.get("group"))
                    has_agg_only = bool(tree.find(exp.AggFunc)) and not has_group
                    if not has_agg_only:
                        issues.append(
                            AIIssue(
                                line=1,
                                column=1,
                                severity="warning",
                                category="performance",
                                message="Запрос не содержит ограничения LIMIT. При большой таблице возможна перегрузка памяти драйвера.",
                                rule="no-limit-clause",
                                suggestion="Добавьте 'LIMIT 1000' для безопасного интерактивного исследования данных.",
                            )
                        )
                        complexity_points += 1.5
                        complexity_notes.append("Неограниченный размер результирующей выборки (без LIMIT)")

            # D. Рекомендация APPROX_DISTINCT вместо COUNT(DISTINCT) в Trino
            if sql_dialect == "trino":
                for count_node in tree.find_all(exp.Count):
                    if (
                        isinstance(count_node.this, exp.Distinct)
                        or count_node.find(exp.Distinct)
                        or count_node.args.get("distinct")
                    ):
                        issues.append(
                            AIIssue(
                                line=1,
                                column=1,
                                severity="info",
                                category="performance",
                                message="Обнаружен точный подсчет уникальных значений COUNT(DISTINCT ...).",
                                rule="approx-distinct-recommendation",
                                suggestion="В Trino рекомендуется использовать 'APPROX_DISTINCT(col, 0.02)' — это ускорит вычисление в 10–50 раз с погрешностью до 2%.",
                            )
                        )
                        complexity_points += 1.5
                        complexity_notes.append("Точный подсчет кардинальности COUNT(DISTINCT)")

            # E. Проверка UNION vs UNION ALL
            for union_node in tree.find_all(exp.Union):
                if union_node.args.get("distinct", True):
                    issues.append(
                        AIIssue(
                            line=1,
                            column=1,
                            severity="warning",
                            category="performance",
                            message="Использование UNION (с неявной дедупликацией) требует тяжелой сортировки данных между узлами.",
                            rule="union-vs-union-all",
                            suggestion="Если гарантия уникальности не требуется или наборы не пересекаются, используйте 'UNION ALL'.",
                        )
                    )
                    complexity_points += 2.0
                    complexity_notes.append("Тяжелая межсетевая дедупликация через UNION")

            # F. Бесполезный ORDER BY во вложенных подзапросах
            for subquery in tree.find_all(exp.Subquery):
                sub_select = subquery.this
                if (
                    isinstance(sub_select, exp.Select)
                    and sub_select.args.get("order")
                    and not sub_select.args.get("limit")
                ):
                    issues.append(
                        AIIssue(
                            line=1,
                            column=1,
                            severity="warning",
                            category="performance",
                            message="Вложенный подзапрос содержит ORDER BY без LIMIT. Сортировка будет проигнорирована во внешнем запросе, но нагрузит кластер.",
                            rule="subquery-order-by",
                            suggestion="Удалите ORDER BY из внутреннего подзапроса или перенесите его в самый верхний запрос.",
                        )
                    )

            # G. Проверка CROSS JOIN / Декартова произведения
            joins = list(tree.find_all(exp.Join))
            if joins:
                complexity_points += len(joins) * 1.5
                complexity_notes.append(f"Количество соединений таблиц (JOIN): {len(joins)}")

            for join in joins:
                kind = (join.args.get("kind") or "").upper()
                if "CROSS" in kind or (not join.args.get("on") and not join.args.get("using")):
                    issues.append(
                        AIIssue(
                            line=1,
                            column=1,
                            severity="warning",
                            category="performance",
                            message="Обнаружен CROSS JOIN (декартово произведение строк).",
                            rule="cross-join-warning",
                            suggestion="Проверьте условие соединения ON. Декартово произведение на больших таблицах вызывает Out Of Memory.",
                        )
                    )
                    complexity_points += 3.0
                    complexity_notes.append("Декартово произведение таблиц (CROSS JOIN)")

            # H. Проверка функций над партициями в WHERE (Partition Pruning breakage)
            for where_node in tree.find_all(exp.Where):
                for func in where_node.find_all(exp.Func):
                    func_name = func.key.upper()
                    if func_name in ("DATE_FORMAT", "YEAR", "MONTH", "DAY", "SUBSTR", "SUBSTRING", "TO_CHAR"):
                        issues.append(
                            AIIssue(
                                line=1,
                                column=1,
                                severity="warning",
                                category="performance",
                                message=f"Вызов функции '{func_name}()' внутри условия WHERE отключает отсечение партиций (Partition Pruning).",
                                rule="partition-function-call",
                                suggestion="Перепишите условие на диапазон дат: вместо 'YEAR(dt) = 2026' используйте 'dt >= DATE '2026-01-01' AND dt < DATE '2027-01-01''.",
                            )
                        )
                        complexity_points += 1.5
                        complexity_notes.append("Вызовы функций в WHERE ломают отсечение партиций")

            # I. Проверка неэффективного LIKE с ведущим '%'
            for like_node in tree.find_all((exp.Like, exp.ILike)):
                pattern = str(like_node.args.get("expression") or "")
                if pattern.startswith("'%") or pattern.startswith(r'"\%'):
                    issues.append(
                        AIIssue(
                            line=1,
                            column=1,
                            severity="info",
                            category="performance",
                            message="Шаблон LIKE с ведущим символом '%' (например '%текст') приводит к полному перебору всех строк.",
                            rule="leading-wildcard-like",
                            suggestion="По возможности используйте поиск по префиксу ('текст%') или полнотекстовые индексы.",
                        )
                    )

            # J. Проверка диалектных функций
            raw_upper = sql.upper()
            if sql_dialect == "trino":
                if re.search(r"\bNVL\s*\(", raw_upper):
                    issues.append(
                        AIIssue(
                            line=1,
                            column=1,
                            severity="error",
                            category="dialect",
                            message="Функция NVL() не поддерживается в Trino SQL.",
                            rule="trino-unsupported-nvl",
                            suggestion="Используйте стандартную ANSI-функцию COALESCE(val, default).",
                        )
                    )
                if re.search(r"\bIFNULL\s*\(", raw_upper):
                    issues.append(
                        AIIssue(
                            line=1,
                            column=1,
                            severity="error",
                            category="dialect",
                            message="Функция IFNULL() не поддерживается в Trino SQL.",
                            rule="trino-unsupported-ifnull",
                            suggestion="Используйте COALESCE(expr1, expr2).",
                        )
                    )
            elif sql_dialect == "hive":
                if re.search(r"\b(TRY|TRY_CAST)\s*\(", raw_upper):
                    issues.append(
                        AIIssue(
                            line=1,
                            column=1,
                            severity="error",
                            category="dialect",
                            message="Функция TRY / TRY_CAST поддерживается в Trino, но отсутствует в Apache Hive.",
                            rule="hive-unsupported-try",
                            suggestion="Используйте стандартный CAST(col AS type) в Hive.",
                        )
                    )

            # K. Schema-Aware & Fuzzy Column Matching
            tables_in_query = [t.name for t in tree.find_all(exp.Table) if t.name]
            available_columns_by_table: Dict[str, Set[str]] = {}

            # Собираем метаданные из catalog_context или MOCK_CATALOGS
            all_known_columns: Set[str] = set()
            for cat_name, schemas in MOCK_CATALOGS.items():
                for sch_name, tables in schemas.items():
                    for tbl_name, cols in tables.items():
                        col_names = {c["name"].lower() for c in cols}
                        available_columns_by_table[tbl_name.lower()] = col_names
                        all_known_columns.update(col_names)

            for col_node in tree.find_all(exp.Column):
                col_name = col_node.name.lower()
                # Проверяем, если колонка не найдена в известных таблицах
                if all_known_columns and col_name not in all_known_columns and col_name not in ("*", ""):
                    close_matches = difflib.get_close_matches(col_name, list(all_known_columns), n=2, cutoff=0.6)
                    hint = f" Возможно, вы имели в виду '{close_matches[0]}'?" if close_matches else ""
                    issues.append(
                        AIIssue(
                            line=1,
                            column=1,
                            severity="warning",
                            category="schema",
                            message=f"Колонка '{col_node.name}' не найдена в схеме данных.{hint}",
                            rule="schema-unknown-column",
                            suggestion=f"Проверьте правильность написания имени колонки.{hint}",
                        )
                    )

        # Расчет индекса сложности (Complexity Score 1-10)
        score = min(10, max(1, int(round(complexity_points))))
        if score <= 3:
            level = "Низкая"
        elif score <= 6:
            level = "Средняя"
        elif score <= 8:
            level = "Высокая"
        else:
            level = "Критическая"

        errors_count = sum(1 for i in issues if i.severity == "error")
        warnings_count = sum(1 for i in issues if i.severity == "warning")
        is_valid = errors_count == 0

        summary = f"Анализ завершен (Сложность: {score}/10, {level}): найдено {errors_count} критических ошибок, {warnings_count} предупреждений."
        if is_valid and not issues:
            summary = f"Запрос прошел проверку (Сложность: {score}/10, {level}): синтаксических ошибок и антипаттернов не обнаружено."

        return AICheckResponse(
            is_valid=is_valid,
            issues=issues,
            summary=summary,
            complexity_score=score,
            complexity_level=level,
            estimated_notes=complexity_notes,
            model="ast-sqlglot-analyzer",
            provider="mock",
            fallback_used=True,
        )

    @classmethod
    def format_sql(cls, sql: str, dialect: str = "trino") -> AIFormatResponse:
        """Красивое автоформатирование SQL с отступами и выравниванием"""
        sql_dialect = cls._map_dialect(dialect)
        try:
            formatted = sqlglot.transpile(sql.strip(), read=sql_dialect, write=sql_dialect, pretty=True)[0]
        except Exception:
            formatted = sql.strip()

        return AIFormatResponse(original_sql=sql, formatted_sql=formatted, dialect=sql_dialect)

    @classmethod
    def explain(cls, sql: str, dialect: str = "trino") -> AIExplainResponse:
        sql_dialect = cls._map_dialect(dialect)
        tables = []
        operations = []

        try:
            parsed = sqlglot.parse_one(sql.strip(), read=sql_dialect)
            tables = list(dict.fromkeys([t.sql() for t in parsed.find_all(exp.Table) if t.name]))
            if parsed.find(exp.With):
                operations.append("CTE (Именованные подзапросы WITH)")
            if parsed.find(exp.Join):
                operations.append("Объединение таблиц (JOIN)")
            if parsed.find(exp.Where):
                operations.append("Фильтрация строк (WHERE)")
            if parsed.find(exp.Group):
                operations.append("Группировка и агрегация (GROUP BY)")
            if parsed.find(exp.Order):
                operations.append("Сортировка результатов (ORDER BY)")
            if parsed.find(exp.Limit):
                operations.append("Ограничение выборки (LIMIT)")
            if parsed.find(exp.Window):
                operations.append("Оконные функции (OVER / PARTITION BY)")
        except Exception:
            tables = list(dict.fromkeys(re.findall(r"\bFROM\s+([a-zA-Z0-9_\.]+)", sql, re.IGNORECASE)))

        explanation_lines = [
            f"### Разбор SQL-запроса ({sql_dialect.upper()})",
            "",
            "#### 1. Задействованные таблицы:",
        ]
        if tables:
            for t in tables:
                explanation_lines.append(f"- `{t}`")
        else:
            explanation_lines.append("- Таблицы не указаны или вычисляются динамически.")

        explanation_lines.extend(
            [
                "",
                "#### 2. Пошаговая логика выполнения:",
            ]
        )
        step = 1
        if "CTE (Именованные подзапросы WITH)" in operations:
            explanation_lines.append(f"{step}. **CTE**: Формируются промежуточные временные наборы данных (WITH).")
            step += 1
        if tables:
            explanation_lines.append(f"{step}. **Чтение данных**: Считываются данные из источника `{tables[0]}`.")
            step += 1
        if "Объединение таблиц (JOIN)" in operations:
            explanation_lines.append(f"{step}. **JOIN**: Выполняется связывание данных с дополнительными таблицами.")
            step += 1
        if "Фильтрация строк (WHERE)" in operations:
            explanation_lines.append(f"{step}. **Фильтрация**: Отсекаются строки, не удовлетворяющие условиям WHERE.")
            step += 1
        if "Группировка и агрегация (GROUP BY)" in operations:
            explanation_lines.append(f"{step}. **Группировка**: Рассчитываются агрегатные функции для групп строк.")
            step += 1
        if "Оконные функции (OVER / PARTITION BY)" in operations:
            explanation_lines.append(
                f"{step}. **Оконные функции**: Производятся расчеты по секциям строк без схлопывания выборки."
            )
            step += 1
        if "Сортировка результатов (ORDER BY)" in operations:
            explanation_lines.append(f"{step}. **Сортировка**: Данные упорядочиваются согласно блоку ORDER BY.")
            step += 1
        if "Ограничение выборки (LIMIT)" in operations:
            explanation_lines.append(f"{step}. **Ограничение**: Возвращаются первые N записей.")
            step += 1

        return AIExplainResponse(
            explanation="\n".join(explanation_lines),
            summary=f"Запрос выполняет аналитическую выборку из {len(tables)} табл. Ключевые этапы: {', '.join(operations) if operations else 'простое чтение'}.",
            tables_used=tables,
            operations=operations,
            model="ast-sqlglot-analyzer",
            provider="mock",
        )

    @classmethod
    def optimize(
        cls, sql: str, dialect: str = "trino", catalog_context: Optional[Dict[str, Any]] = None
    ) -> AIOptimizeResponse:
        sql_dialect = cls._map_dialect(dialect)
        optimizations: List[str] = []
        original_stripped = sql.strip()
        optimized = original_stripped

        # 1. Замена NVL на COALESCE
        if sql_dialect == "trino" and re.search(r"\bNVL\s*\(", optimized, re.IGNORECASE):
            optimized = re.sub(r"\bNVL\s*\(", "COALESCE(", optimized, flags=re.IGNORECASE)
            optimizations.append("Замена диалектной функции NVL() на стандартную Trino COALESCE().")

        # 2. Добавление LIMIT при отсутствии
        if not re.search(r"\bLIMIT\s+\d+", optimized, re.IGNORECASE) and not re.search(
            r"\bCOUNT\s*\(", optimized, re.IGNORECASE
        ):
            if not optimized.endswith(";"):
                optimized += "\nLIMIT 1000;"
            else:
                optimized = optimized[:-1].strip() + "\nLIMIT 1000;"
            optimizations.append("Добавлено ограничение LIMIT 1000 для защиты от переполнения памяти драйвера.")

        # 3. Замена COUNT(DISTINCT) на APPROX_DISTINCT в Trino
        if sql_dialect == "trino" and re.search(
            r"\bCOUNT\s*\(\s*DISTINCT\s+([a-zA-Z0-9_]+)\s*\)", optimized, re.IGNORECASE
        ):
            optimized = re.sub(
                r"\bCOUNT\s*\(\s*DISTINCT\s+([a-zA-Z0-9_]+)\s*\)",
                r"APPROX_DISTINCT(\1, 0.02)",
                optimized,
                flags=re.IGNORECASE,
            )
            optimizations.append(
                "Замена точного COUNT(DISTINCT) на высокоскоростной APPROX_DISTINCT(col, 0.02) для Trino."
            )

        # 4. Красивое форматирование
        try:
            formatted = sqlglot.transpile(optimized, read=sql_dialect, write=sql_dialect, pretty=True)[0]
            if formatted.strip() != original_stripped and not optimizations:
                optimizations.append("Автоматическое форматирование SQL структуры (UPPERCASE ключевые слова, отступы).")
            optimized = formatted
        except Exception:
            pass

        if optimized.strip() == original_stripped:
            optimizations = []
            diff_summary = "Запрос уже оптимизирован, изменений не требуется."
        else:
            diff_summary = f"Применено оптимизаций: {len(optimizations)}."

        return AIOptimizeResponse(
            original_sql=sql,
            optimized_sql=optimized,
            optimizations=optimizations,
            diff_summary=diff_summary,
            model="ast-sqlglot-analyzer",
            provider="mock",
        )

    @classmethod
    def fix(cls, sql: str, dialect: str = "trino", error_message: str = "") -> AIFixResponse:
        sql_dialect = cls._map_dialect(dialect)
        fixed = sql.strip()
        explanation = "Произведен автоматический анализ ошибки выполнения."

        error_lower = (error_message or "").lower()

        if "function nvl not registered" in error_lower or "cannot resolve nvl" in error_lower:
            fixed = re.sub(r"\bNVL\s*\(", "COALESCE(", fixed, flags=re.IGNORECASE)
            explanation = (
                "Ошибка вызвана отсутствием функции NVL в Trino. Выполнена автоматическая замена на `COALESCE(...)`."
            )
        elif "unexpected limit" in error_lower or "syntax error" in error_lower:
            fixed = re.sub(r";\s*;", ";", fixed)
            explanation = "Исправлены дублирующиеся разделители и синтаксические ошибки."
        else:
            explanation = f"На основе текста ошибки ('{error_message[:100]}...') произведено форматирование и проверка конструкций {sql_dialect.upper()}."

        # Форматируем исправленный запрос
        try:
            fixed = sqlglot.transpile(fixed, read=sql_dialect, write=sql_dialect, pretty=True)[0]
        except Exception:
            pass

        return AIFixResponse(
            original_sql=sql, fixed_sql=fixed, explanation=explanation, model="ast-sqlglot-analyzer", provider="mock"
        )

    @classmethod
    def generate(
        cls, prompt: str, dialect: str = "trino", catalog_context: Optional[Dict[str, Any]] = None
    ) -> AIGenerateResponse:
        """
        Детерминированный генератор SQL-запросов по тексту на естественном языке (Mock/Offline режим).
        Учитывает партиционирование таблиц (Partition Pruning) и оптимизирован для распределенного выполнения на YARN.
        """
        sql_dialect = cls._map_dialect(dialect)
        p_lower = prompt.lower().strip()

        # Поиск лимита в тексте (например: "топ 5", "топ 10", "первые 20", "limit 50")
        limit_match = re.search(r"\b(?:топ|top|первые|first|limit)\s+(\d+)\b", p_lower)
        limit = int(limit_match.group(1)) if limit_match else 100

        # Поиск временного диапазона
        days_match = re.search(r"(\d+)\s*(?:дней|дня|день|days|day)", p_lower)
        days = int(days_match.group(1)) if days_match else 30

        generated_sql = ""
        explanation = ""
        tables_used: List[str] = []

        # 1. Запросы по клиентам и заказам (Customer + Orders / Join)
        if any(w in p_lower for w in ("клиент", "покупател", "customer", "пользовател")) and any(
            w in p_lower for w in ("заказ", "order", "покупк", "трат", "spent", "выручк")
        ):
            tables_used = ["tpch.sf1.customer", "tpch.sf1.orders"]
            date_filter = (
                "o.orderdate >= DATE '1998-01-01'" if sql_dialect == "trino" else "o.orderdate >= '1998-01-01'"
            )
            generated_sql = f"""
SELECT
    c.custkey,
    c.name,
    c.mktsegment,
    COUNT(o.orderkey) AS total_orders,
    ROUND(SUM(o.totalprice), 2) AS total_spent
FROM tpch.sf1.customer c
JOIN tpch.sf1.orders o ON c.custkey = o.custkey
WHERE {date_filter}
GROUP BY c.custkey, c.name, c.mktsegment
ORDER BY total_spent DESC
LIMIT {limit};
            """
            explanation = (
                f"### Оптимизация для YARN и HDFS:\n"
                f"1. **Partition Pruning**: Условие `{date_filter}` выполняет отсечение партиций на уровне метастора/HDFS, минимизируя считываемые сплиты и число запускаемых YARN Mappers.\n"
                f"2. **Колоночная проекция**: Выбраны только необходимые атрибуты клиентов и заказов без SELECT *, снижая десериализацию ORC/Parquet в память YARN-контейнеров.\n"
                f"3. **Защита от 1-Reducer Bottleneck**: Добавлено ограничение `LIMIT {limit}`, позволяющее локально аккумулировать Top-N на узлах без перегрузки единственного Reducer."
            )

        # 2. Запросы по клиентам и балансу / сегментам
        elif any(w in p_lower for w in ("клиент", "покупател", "customer")):
            tables_used = ["tpch.sf1.customer"]
            if any(w in p_lower for w in ("сегмент", "mktsegment", "групп")):
                generated_sql = f"""
SELECT
    mktsegment,
    COUNT(*) AS total_customers,
    ROUND(AVG(acctbal), 2) AS avg_account_balance,
    ROUND(SUM(acctbal), 2) AS total_balance
FROM tpch.sf1.customer
GROUP BY mktsegment
ORDER BY total_customers DESC
LIMIT {limit};
                """
                explanation = (
                    "### Оптимизация для YARN:\n"
                    "1. Агрегация по срезам `mktsegment` использует Hash Aggregation внутри контейнеров YARN.\n"
                    f"2. Ограничение `LIMIT {limit}` защищает память драйвера от переполнения."
                )
            elif any(w in p_lower for w in ("топ", "богат", "наибольш", "баланс", "acctbal", "крупн")):
                generated_sql = f"""
SELECT
    custkey,
    name,
    mktsegment,
    phone,
    acctbal
FROM tpch.sf1.customer
WHERE acctbal > 0
ORDER BY acctbal DESC
LIMIT {limit};
                """
                explanation = (
                    f"### Оптимизация для YARN:\n"
                    f"1. Предикат фильтрации `acctbal > 0` отсекает нулевые/отрицательные записи до сортировки.\n"
                    f"2. Сортировка с `LIMIT {limit}` выполняется через распределенный Top-K в YARN Task-трекерах."
                )
            else:
                generated_sql = f"""
SELECT
    custkey,
    name,
    address,
    phone,
    acctbal,
    mktsegment
FROM tpch.sf1.customer
LIMIT {limit};
                """
                explanation = f"Базовая выборка с явной проекцией колонок и безопасным `LIMIT {limit}` для работы в интерактивной YARN сессии."

        # 3. Запросы по заказам (Orders)
        elif any(w in p_lower for w in ("заказ", "order", "покупк")):
            tables_used = ["tpch.sf1.orders"]
            date_filter = "orderdate >= DATE '1998-01-01'" if sql_dialect == "trino" else "orderdate >= '1998-01-01'"

            if any(w in p_lower for w in ("статус", "status", "orderstatus", "групп")):
                generated_sql = f"""
SELECT
    orderstatus,
    COUNT(*) AS orders_count,
    ROUND(SUM(totalprice), 2) AS total_revenue,
    ROUND(AVG(totalprice), 2) AS avg_order_value
FROM tpch.sf1.orders
WHERE {date_filter}
GROUP BY orderstatus
ORDER BY orders_count DESC;
                """
                explanation = (
                    f"### Оптимизация для YARN и Hive/Trino:\n"
                    f"1. **Partition Pruning**: Предикат `{date_filter}` на партиционной колонке дат `orderdate` отсекает исторические разделы HDFS.\n"
                    f"2. **Map-Side Aggregation**: Группировка по небольшому числу ключей статусов эффективно использует combiner/map-side агрегацию в YARN Mappers."
                )
            elif any(w in p_lower for w in ("приоритет", "priority", "orderpriority")):
                generated_sql = f"""
SELECT
    orderpriority,
    COUNT(*) AS count_orders,
    ROUND(SUM(totalprice), 2) AS revenue
FROM tpch.sf1.orders
WHERE {date_filter}
GROUP BY orderpriority
ORDER BY revenue DESC;
                """
                explanation = f"Агрегация с отсечением партиций `{date_filter}` и мап-сайд предварительным суммированием в контейнерах YARN."
            elif any(w in p_lower for w in ("топ", "дорог", "крупн", "totalprice", "наибольш")):
                generated_sql = f"""
SELECT
    orderkey,
    custkey,
    orderstatus,
    totalprice,
    orderdate,
    orderpriority
FROM tpch.sf1.orders
WHERE {date_filter}
ORDER BY totalprice DESC
LIMIT {limit};
                """
                explanation = (
                    f"### Оптимизация для YARN:\n"
                    f"1. **Partition Elimination**: Фильтр `{date_filter}` отсекает ненужные файлы с HDFS.\n"
                    f"2. **Top-N Pushdown**: `LIMIT {limit}` передается в Execution Plan, предотвращая перегрузку единственного Reducer."
                )
            else:
                generated_sql = f"""
SELECT
    orderkey,
    custkey,
    orderstatus,
    totalprice,
    orderdate
FROM tpch.sf1.orders
WHERE {date_filter}
ORDER BY orderdate DESC
LIMIT {limit};
                """
                explanation = (
                    f"Выборка последних заказов с отсечением партиций по дате `{date_filter}` и лимитом {limit} строк."
                )

        # 4. Метрики активности / DAU / Платформы
        elif any(w in p_lower for w in ("dau", "метрик", "платформ", "активн", "platform", "сесси")):
            tables_used = ["analytics.events.dau_metrics"]
            date_filter = (
                f"report_date >= CURRENT_DATE - INTERVAL '{days}' DAY"
                if sql_dialect == "trino"
                else f"report_date >= DATE_SUB(CURRENT_DATE(), {days})"
            )

            generated_sql = f"""
SELECT
    report_date,
    platform,
    active_users,
    ROUND(avg_session_sec / 60.0, 2) AS avg_session_minutes
FROM analytics.events.dau_metrics
WHERE {date_filter}
ORDER BY report_date DESC, active_users DESC
LIMIT {limit};
            """
            explanation = (
                f"### Оптимизация для YARN:\n"
                f"1. **Partition Pruning**: Строгий фильтр `{date_filter}` по партиционной колонке `report_date` исключает чтение старых партиций HDFS.\n"
                f"2. **Минимизация I/O**: Запрос обращается только к целевым колонкам ORC/Parquet файла в HDFS."
            )

        # 5. Логи действий / События пользователей (User Actions)
        elif any(w in p_lower for w in ("действи", "событи", "action", "event", "клик", "лог")):
            tables_used = ["analytics.events.user_actions"]
            date_filter = (
                f"created_at >= CURRENT_TIMESTAMP - INTERVAL '{days}' DAY"
                if sql_dialect == "trino"
                else f"created_at >= DATE_SUB(CURRENT_TIMESTAMP(), {days})"
            )

            if any(w in p_lower for w in ("тип", "групп", "частот", "популярн", "категори")):
                approx_clause = (
                    "APPROX_DISTINCT(user_id, 0.02)" if sql_dialect == "trino" else "COUNT(DISTINCT user_id)"
                )
                generated_sql = f"""
SELECT
    event_type,
    COUNT(*) AS total_events,
    {approx_clause} AS unique_users
FROM analytics.events.user_actions
WHERE {date_filter}
GROUP BY event_type
ORDER BY total_events DESC;
                """
                explanation = (
                    f"### Оптимизация для YARN:\n"
                    f"1. **Partition Pruning**: Отсечение партиций `{date_filter}` исключает сканирование терабайт логов на HDFS.\n"
                    f"2. **HyperLogLog / Skew Protection**: Использование `{approx_clause}` предотвращает Data Skew и взрыв памяти YARN контейнеров при подсчете уникальных пользователей."
                )
            elif any(w in p_lower for w in ("пользовател", "активн", "топ", "user_id")):
                generated_sql = f"""
SELECT
    user_id,
    COUNT(*) AS actions_count,
    COUNT(DISTINCT event_type) AS event_types_count,
    MAX(created_at) AS last_action_time
FROM analytics.events.user_actions
WHERE {date_filter}
GROUP BY user_id
ORDER BY actions_count DESC
LIMIT {limit};
                """
                explanation = (
                    f"### Оптимизация для YARN:\n"
                    f"1. Партиционирование по дате `{date_filter}` защищает кластер YARN от Full Scan.\n"
                    f"2. Ограничение `LIMIT {limit}` позволяет провести Top-N редукцию без исчерпания памяти Reducer."
                )
            else:
                generated_sql = f"""
SELECT
    event_id,
    user_id,
    event_type,
    created_at,
    ip_address
FROM analytics.events.user_actions
WHERE {date_filter}
ORDER BY created_at DESC
LIMIT {limit};
                """
                explanation = (
                    f"Выборка журнала событий с партиционным фильтром `{date_filter}` и ограничением в {limit} строк."
                )

        # 6. Универсальный фолбэк по контексту схемы
        else:
            matched_table = None
            for cat, schemas in MOCK_CATALOGS.items():
                for sch, tbls in schemas.items():
                    for tbl in tbls.keys():
                        full_name = f"{cat}.{sch}.{tbl}"
                        if tbl in p_lower or full_name in p_lower:
                            matched_table = full_name
                            break

            if matched_table:
                tables_used = [matched_table]
                generated_sql = f"""
SELECT *
FROM {matched_table}
LIMIT {limit};
                """
                explanation = f"Сформирован запрос к `{matched_table}` с безопасным ограничением `LIMIT {limit}` для работы на YARN."
            else:
                tables_used = ["tpch.sf1.customer"]
                generated_sql = f"""
SELECT
    custkey,
    name,
    mktsegment,
    acctbal
FROM tpch.sf1.customer
ORDER BY acctbal DESC
LIMIT {limit};
                """
                explanation = (
                    f"### Оптимизация для YARN:\n"
                    f"Сгенерирован аналитический шаблон для {sql_dialect.upper()}: явный список колонок, "
                    f"сортировка Top-K с лимитом {limit} для оптимизации распределения по контейнерам YARN."
                )

        # Форматирование через sqlglot
        try:
            formatted = sqlglot.transpile(generated_sql.strip(), read=sql_dialect, write=sql_dialect, pretty=True)[0]
            generated_sql = formatted
        except Exception:
            generated_sql = generated_sql.strip()

        return AIGenerateResponse(
            prompt=prompt,
            generated_sql=generated_sql,
            explanation=explanation,
            tables_used=tables_used,
            model="ast-sqlglot-analyzer",
            provider="mock",
            fallback_used=True,
        )


# --- Сервис взаимодействия с On-premise LLM и fallback на AST ---


class AIService:
    def __init__(self, config: Optional[AIConfig] = None):
        self.config = config or settings.ai

    def _get_client(self) -> httpx.AsyncClient:
        headers = {"Content-Type": "application/json"}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        return httpx.AsyncClient(
            base_url=self.config.base_url.rstrip("/"),
            headers=headers,
            timeout=httpx.Timeout(self.config.timeout_seconds, connect=5.0),
        )

    async def get_status(self) -> AIStatusResponse:
        """Проверка доступности On-premise LLM эндпоинта"""
        if not self.config.enabled:
            return AIStatusResponse(
                enabled=False,
                provider=self.config.provider,
                model=self.config.model,
                base_url=self.config.base_url,
                available=False,
                message="ИИ-ассистент отключен в конфигурации.",
            )

        if self.config.provider == "mock":
            return AIStatusResponse(
                enabled=True,
                provider="mock",
                model="ast-sqlglot-analyzer",
                base_url="local://in-memory",
                available=False,  # Явно false для mock режима
                message="Используется встроенный AST-анализатор (Mock).",
            )

        start_time = time.time()
        try:
            async with self._get_client() as client:
                resp = await client.get("/models")
                latency = round((time.time() - start_time) * 1000, 2)
                if resp.status_code == 200:
                    data = resp.json()
                    models = [m.get("id") for m in data.get("data", []) if isinstance(m, dict)]
                    model_info = (
                        f"Модель '{self.config.model}' активна"
                        if self.config.model in models
                        else f"Доступны модели: {', '.join(models[:3])}"
                    )
                    return AIStatusResponse(
                        enabled=True,
                        provider=self.config.provider,
                        model=self.config.model,
                        base_url=self.config.base_url,
                        available=True,
                        message=f"Подключено к On-premise LLM ({model_info}).",
                        latency_ms=latency,
                    )
                else:
                    return AIStatusResponse(
                        enabled=True,
                        provider=self.config.provider,
                        model=self.config.model,
                        base_url=self.config.base_url,
                        available=False,
                        message=f"LLM сервер ответил кодом {resp.status_code}. Активен встроенный AST-анализатор.",
                        latency_ms=latency,
                    )
        except Exception as e:
            return AIStatusResponse(
                enabled=True,
                provider=self.config.provider,
                model=self.config.model,
                base_url=self.config.base_url,
                available=False,
                message=f"On-premise LLM недоступен ({str(e)[:60]}). Активен встроенный AST-анализатор.",
            )

    async def check_query(
        self, sql: str, dialect: str = "trino", catalog_context: Optional[Dict[str, Any]] = None
    ) -> AICheckResponse:
        """Комплексная проверка и линтинг SQL запроса"""
        clean_sql = sanitize_prompt_input(sql)
        if not self.config.enabled or self.config.provider == "mock":
            return MockSQLAnalyzer.check(clean_sql, dialect, catalog_context)

        start_time = time.time()
        system_prompt = (
            f"Ты — экспертный SQL Linter для {dialect.upper()} (Trino / Apache Hive).\n"
            "Проведи аудит SQL-запроса и верни JSON со списком замечаний и индексом сложности (1-10):\n"
            "{\n"
            '  "is_valid": true/false,\n'
            '  "summary": "Краткое резюме на русском языке",\n'
            '  "complexity_score": 1-10,\n'
            '  "complexity_level": "Низкая" | "Средняя" | "Высокая" | "Критическая",\n'
            '  "estimated_notes": ["примечание1", "примечание2"],\n'
            '  "issues": [\n'
            "    {\n"
            '      "line": 1,\n'
            '      "column": 1,\n'
            '      "severity": "error" | "warning" | "info",\n'
            '      "category": "syntax" | "performance" | "schema" | "security" | "dialect",\n'
            '      "message": "Описание проблемы",\n'
            '      "rule": "идентификатор-правила",\n'
            '      "suggestion": "Рекомендация"\n'
            "    }\n"
            "  ]\n"
            "}\n"
            "Только валидный JSON!"
        )

        try:
            async with self._get_client() as client:
                payload = {
                    "model": self.config.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": f"Диалект: {dialect}\nSQL:\n```sql\n{clean_sql}\n```"},
                    ],
                    "temperature": self.config.temperature,
                    "max_tokens": self.config.max_tokens,
                    "response_format": {"type": "json_object"},
                }
                resp = await client.post("/chat/completions", json=payload)
                if resp.status_code == 200:
                    parsed = json.loads(resp.json()["choices"][0]["message"]["content"])
                    raw_issues = parsed.get("issues", [])
                    issues = [AIIssue(**i) for i in raw_issues]
                    elapsed = round((time.time() - start_time) * 1000, 2)
                    return AICheckResponse(
                        is_valid=parsed.get("is_valid", len([i for i in issues if i.severity == "error"]) == 0),
                        issues=issues,
                        summary=parsed.get("summary", "Анализ завершен успешно."),
                        complexity_score=parsed.get("complexity_score", 1),
                        complexity_level=parsed.get("complexity_level", "Низкая"),
                        estimated_notes=parsed.get("estimated_notes", []),
                        model=self.config.model,
                        provider="on-premise-llm",
                        execution_time_ms=elapsed,
                        fallback_used=False,
                    )
                else:
                    res = MockSQLAnalyzer.check(clean_sql, dialect, catalog_context)
                    res.execution_time_ms = round((time.time() - start_time) * 1000, 2)
                    return res
        except Exception:
            res = MockSQLAnalyzer.check(clean_sql, dialect, catalog_context)
            res.execution_time_ms = round((time.time() - start_time) * 1000, 2)
            return res

    async def format_sql(self, sql: str, dialect: str = "trino") -> AIFormatResponse:
        clean_sql = sanitize_prompt_input(sql)
        return MockSQLAnalyzer.format_sql(clean_sql, dialect)

    async def explain_query(self, sql: str, dialect: str = "trino") -> AIExplainResponse:
        clean_sql = sanitize_prompt_input(sql)
        if not self.config.enabled or self.config.provider == "mock":
            return MockSQLAnalyzer.explain(clean_sql, dialect)
        try:
            async with self._get_client() as client:
                system_prompt = f"Ты — эксперт по SQL {dialect.upper()}. Объясни логику запроса на русском языке в JSON с полями summary, tables_used, operations, explanation."
                resp = await client.post(
                    "/chat/completions",
                    json={
                        "model": self.config.model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": f"SQL:\n```sql\n{clean_sql}\n```"},
                        ],
                        "temperature": 0.2,
                        "max_tokens": self.config.max_tokens,
                        "response_format": {"type": "json_object"},
                    },
                )
                if resp.status_code == 200:
                    parsed = json.loads(resp.json()["choices"][0]["message"]["content"])
                    return AIExplainResponse(
                        explanation=parsed.get("explanation", ""),
                        summary=parsed.get("summary", ""),
                        tables_used=parsed.get("tables_used", []),
                        operations=parsed.get("operations", []),
                        model=self.config.model,
                        provider="on-premise-llm",
                    )
        except Exception:
            pass
        return MockSQLAnalyzer.explain(clean_sql, dialect)

    async def optimize_query(
        self, sql: str, dialect: str = "trino", catalog_context: Optional[Dict[str, Any]] = None
    ) -> AIOptimizeResponse:
        clean_sql = sanitize_prompt_input(sql)
        return MockSQLAnalyzer.optimize(clean_sql, dialect, catalog_context)

    async def fix_query(self, sql: str, dialect: str = "trino", error_message: str = "") -> AIFixResponse:
        clean_sql = sanitize_prompt_input(sql)
        clean_err = sanitize_prompt_input(error_message, max_chars=5000)
        return MockSQLAnalyzer.fix(clean_sql, dialect, clean_err)

    async def generate_query(
        self, prompt: str, dialect: str = "trino", catalog_context: Optional[Dict[str, Any]] = None
    ) -> AIGenerateResponse:
        """
        Генерация SQL-запроса по естественному языку (Text-to-SQL).
        Использует подключенный On-premise LLM сервис или откатывается к эвристическому AST-генератору.
        """
        clean_prompt = sanitize_prompt_input(prompt, max_chars=10000)
        start_time = time.time()

        if not self.config.enabled or self.config.provider == "mock":
            res = MockSQLAnalyzer.generate(clean_prompt, dialect, catalog_context)
            res.execution_time_ms = round((time.time() - start_time) * 1000, 2)
            return res

        # Подготавливаем контекст схемы для промпта
        schema_desc = []
        target_context = catalog_context or MOCK_CATALOGS
        if isinstance(target_context, dict):
            for cat_name, schemas in list(target_context.items())[:3]:
                if isinstance(schemas, dict):
                    for sch_name, tables in list(schemas.items())[:3]:
                        if isinstance(tables, dict):
                            for tbl_name, cols in list(tables.items())[:5]:
                                if isinstance(cols, list):
                                    col_str = ", ".join(
                                        [
                                            f"{c.get('name', '')} ({c.get('type', '')})"
                                            for c in cols
                                            if isinstance(c, dict)
                                        ][:8]
                                    )
                                    schema_desc.append(f"Таблица {cat_name}.{sch_name}.{tbl_name}: {col_str}")
        schema_prompt_section = (
            "\n".join(schema_desc)
            if schema_desc
            else "Доступны таблицы: tpch.sf1.customer, tpch.sf1.orders, analytics.events.user_actions, analytics.events.dau_metrics"
        )

        system_prompt = (
            f"Ты — ведущий инженер данных и эксперт по генерации аналитических SQL-запросов (Text-to-SQL) для диалекта {dialect.upper()} (Trino / Apache Hive на YARN).\n"
            "Пользователь передает задачу на естественном языке и контекст схемы данных.\n"
            "Твоя задача — сгенерировать точный, высокопроизводительный и безопасный SQL-запрос, оптимизированный для работы в распределенной среде Hadoop / YARN.\n\n"
            "КРИТИЧЕСКИ ВАЖНЫЕ ТРЕБОВАНИЯ К ОПТИМИЗАЦИИ ДЛЯ YARN И HDFS:\n"
            "1. ПАРТИЦИОНИРОВАНИЕ (Partition Pruning):\n"
            "   - Всегда добавляй предикаты фильтрации по партиционным колонкам дат/времени (dt, date, report_date, orderdate, created_at, p_date) в блоке WHERE.\n"
            "   - Запрещено вызывать скалярные функции над колонкой партиции (НЕЛЬЗЯ year(dt)=2026, НУЖНО dt >= DATE '2026-01-01' AND dt < DATE '2027-01-01'). Это критично для статического отсечения директорий HDFS до выделения YARN контейнеров (Mappers).\n"
            "2. ОПТИМИЗАЦИЯ РЕСУРСОВ YARN:\n"
            "   - Проекция колонок (Column Pruning): избегай 'SELECT *' на больших таблицах, перечисляй только нужные поля (меньше I/O и десериализации ORC/Parquet в память контейнеров).\n"
            "   - Защита от перегрузки единственного Reducer: никогда не используй глобальный 'ORDER BY' без 'LIMIT'. Всегда добавляй разумный LIMIT (например, LIMIT 100 или 1000).\n"
            "   - Минимизация Shuffle: фильтруй данные до JOIN (Predicate Pushdown). В Trino используй APPROX_DISTINCT(col, 0.02) вместо точного COUNT(DISTINCT) для предотвращения Data Skew в YARN NodeManager.\n"
            "3. БЕЗОПАСНОСТЬ:\n"
            "   - Запрос должен быть СТРОГО READ-ONLY (SELECT или WITH CTE).\n"
            "   - КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНЫ любые деструктивные команды (INSERT, UPDATE, DELETE, DROP, TRUNCATE, ALTER, CREATE, GRANT).\n\n"
            "Верни ответ СТРОГО в формате JSON:\n"
            "{\n"
            '  "generated_sql": "SELECT ...",\n'
            '  "explanation": "Подробное объяснение логики запроса на русском языке с описанием примененных оптимизаций для YARN и Partition Pruning",\n'
            '  "tables_used": ["схема.таблица1", "таблица2"]\n'
            "}\n"
            "Только чистый JSON без markdown обрамления ```json!"
        )

        user_content = (
            f"Диалект: {dialect}\n"
            f"Доступные схемы и таблицы:\n{schema_prompt_section}\n\n"
            f"Запрос пользователя:\n{clean_prompt}"
        )

        try:
            async with self._get_client() as client:
                payload = {
                    "model": self.config.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_content},
                    ],
                    "temperature": 0.2,
                    "max_tokens": self.config.max_tokens,
                    "response_format": {"type": "json_object"},
                }
                resp = await client.post("/chat/completions", json=payload)
                if resp.status_code == 200:
                    raw_content = resp.json()["choices"][0]["message"]["content"]
                    parsed = json.loads(raw_content)
                    sql = parsed.get("generated_sql", "").strip()

                    # Валидация безопасности AST
                    is_safe, err_msg = validate_readonly_sql_ast(sql, dialect)
                    if not is_safe:
                        logger.warning(f"Сгенерированный LLM запрос заблокирован AST-политикой: {err_msg}")
                        res = MockSQLAnalyzer.generate(clean_prompt, dialect, catalog_context)
                        res.explanation += f" (Внимание: запрос LLM был заблокирован политикой безопасности: {err_msg})"
                        res.execution_time_ms = round((time.time() - start_time) * 1000, 2)
                        return res

                    # Автоформатирование
                    try:
                        sql = sqlglot.transpile(
                            sql,
                            read=MockSQLAnalyzer._map_dialect(dialect),
                            write=MockSQLAnalyzer._map_dialect(dialect),
                            pretty=True,
                        )[0]
                    except Exception:
                        pass

                    elapsed = round((time.time() - start_time) * 1000, 2)
                    return AIGenerateResponse(
                        prompt=clean_prompt,
                        generated_sql=sql,
                        explanation=parsed.get("explanation", "Запрос сгенерирован ИИ-ассистентом."),
                        tables_used=parsed.get("tables_used", []),
                        model=self.config.model,
                        provider="on-premise-llm",
                        execution_time_ms=elapsed,
                        fallback_used=False,
                    )
        except Exception as e:
            logger.warning(f"Ошибка при обращении к LLM для генерации SQL: {e}. Применяется Mock-генератор.")

        res = MockSQLAnalyzer.generate(clean_prompt, dialect, catalog_context)
        res.execution_time_ms = round((time.time() - start_time) * 1000, 2)
        return res


ai_service = AIService()
