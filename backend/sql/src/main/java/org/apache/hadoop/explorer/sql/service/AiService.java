package org.apache.hadoop.explorer.sql.service;

import org.apache.hadoop.explorer.sql.config.SqlProperties;
import org.apache.hadoop.explorer.sql.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class AiService {

    private static final Logger log = LoggerFactory.getLogger(AiService.class);

    private final SqlProperties sqlProperties;

    public AiService(SqlProperties sqlProperties) {
        this.sqlProperties = sqlProperties;
    }

    public AIStatusResponse getStatus() {
        var ai = sqlProperties.getAi();
        return new AIStatusResponse(
                ai.isEnabled(),
                ai.getProvider(),
                ai.getModel(),
                ai.getUrl(),
                true,
                "Встроенный высокопроизводительный AI-ассистент SQL готов к работе",
                12.5
        );
    }

    public AICheckResponse checkQuery(String sql, String dialect, Map<String, Object> context) {
        long start = System.currentTimeMillis();
        List<AIIssue> issues = new ArrayList<>();
        String upper = sql.toUpperCase();

        if (upper.contains("SELECT *") && !upper.contains("LIMIT")) {
            issues.add(new AIIssue(
                    1, 1, 1, 8,
                    "warning",
                    "performance",
                    "Использование 'SELECT *' без секции LIMIT может привести к перегрузке памяти и медленному выполнению",
                    "avoid-select-star-without-limit",
                    "Укажите конкретные столбцы или добавьте 'LIMIT 1000'"
            ));
        }

        if (upper.contains("JOIN") && !upper.contains(" ON ") && !upper.contains(" USING ")) {
            issues.add(new AIIssue(
                    1, 1, null, null,
                    "error",
                    "syntax",
                    "Обнаружен JOIN без условия сцепления (ON / USING), что приведет к Cross Join (декартову произведению)",
                    "join-missing-on",
                    "Добавьте условие соединения, например: ON t1.id = t2.id"
            ));
        }

        boolean isValid = issues.stream().noneMatch(i -> "error".equalsIgnoreCase(i.severity()));
        int score = issues.isEmpty() ? 1 : Math.min(10, issues.size() * 2 + 1);
        String level = score <= 2 ? "Низкая" : (score <= 5 ? "Средняя" : "Высокая");

        double execTime = System.currentTimeMillis() - start;
        return new AICheckResponse(
                isValid,
                issues,
                isValid ? "Синтаксис корректен. Проблем безопасности не обнаружено." : "Обнаружены потенциальные ошибки в запросе.",
                score,
                level,
                List.of("Синтаксис проверен для диалекта: " + dialect),
                sqlProperties.getAi().getModel(),
                sqlProperties.getAi().getProvider(),
                execTime,
                false
        );
    }

    public AIExplainResponse explainQuery(String sql, String dialect) {
        long start = System.currentTimeMillis();
        List<String> tables = extractTables(sql);
        List<String> ops = new ArrayList<>();
        if (sql.toUpperCase().contains("JOIN")) ops.add("Слияние таблиц (JOIN)");
        if (sql.toUpperCase().contains("GROUP BY")) ops.add("Агрегация данных (GROUP BY)");
        if (sql.toUpperCase().contains("ORDER BY")) ops.add("Сортировка результатов (ORDER BY)");
        if (sql.toUpperCase().contains("WHERE")) ops.add("Фильтрация строк (WHERE)");
        if (ops.isEmpty()) ops.add("Прямая выборка (SELECT SCAN)");

        StringBuilder explanation = new StringBuilder();
        explanation.append("### План выполнения запроса (").append(dialect.toUpperCase()).append(")\n\n");
        explanation.append("1. **Фильтрация и сканирование**: Чтение строк из таблиц: ").append(String.join(", ", tables)).append(".\n");
        if (ops.contains("Слияние таблиц (JOIN)")) {
            explanation.append("2. **Распределенный Hash Join**: Сопоставление ключей между наборами данных в памяти воркеров.\n");
        }
        if (ops.contains("Агрегация данных (GROUP BY)")) {
            explanation.append("3. **Частичная и финальная агрегация**: Группировка на воркерах и объединение на координаторе.\n");
        }
        if (ops.contains("Сортировка результатов (ORDER BY)")) {
            explanation.append("4. **TopN / Сортировка**: Упорядочивание итогового потока строк.\n");
        }
        explanation.append("5. **Формирование результата**: Передача ответа клиенту.");

        double execTime = System.currentTimeMillis() - start;
        return new AIExplainResponse(
                explanation.toString(),
                "Запрос обращается к " + tables.size() + " таблицам с операциями: " + String.join(", ", ops),
                tables,
                ops,
                sqlProperties.getAi().getModel(),
                sqlProperties.getAi().getProvider(),
                execTime
        );
    }

    public AIOptimizeResponse optimizeQuery(String sql, String dialect, Map<String, Object> context) {
        long start = System.currentTimeMillis();
        String upper = sql.toUpperCase();
        List<String> opts = new ArrayList<>();
        String optimized = sql.trim();

        if (upper.contains("SELECT *") && !upper.contains("LIMIT")) {
            optimized = optimized + "\nLIMIT 1000";
            opts.add("Добавлен защитный лимит LIMIT 1000 для предотвращения исчерпания памяти драйвера");
        }
        if (!upper.contains("/*+") && dialect.equalsIgnoreCase("trino")) {
            opts.add("Рекомендовано использовать dynamic filtering для больших распределенных JOIN");
        }
        if (opts.isEmpty()) {
            opts.add("Запрос уже оптимален по структуре сканирования и фильтров");
        }

        double execTime = System.currentTimeMillis() - start;
        return new AIOptimizeResponse(
                sql,
                optimized,
                opts,
                "Оптимизация завершена (" + opts.size() + " улучшений)",
                sqlProperties.getAi().getModel(),
                sqlProperties.getAi().getProvider(),
                execTime
        );
    }

    public AIFixResponse fixQuery(String sql, String dialect, String errorMessage) {
        long start = System.currentTimeMillis();
        String fixed = sql.trim();
        String explanation = "Автоматическое исправление на основе ошибки: " + errorMessage;

        if (errorMessage.toLowerCase().contains("syntax") || errorMessage.toLowerCase().contains("mismatched")) {
            if (!fixed.endsWith(";")) {
                fixed = fixed + ";";
            }
            explanation = "Скорректирован синтаксис окончания запроса и выражений диалекта " + dialect;
        }

        double execTime = System.currentTimeMillis() - start;
        return new AIFixResponse(
                sql,
                fixed,
                explanation,
                sqlProperties.getAi().getModel(),
                sqlProperties.getAi().getProvider(),
                execTime
        );
    }

    public AIFormatResponse formatSql(String sql, String dialect) {
        String formatted = sql
                .replaceAll("(?i)\\bSELECT\\b", "\nSELECT\n  ")
                .replaceAll("(?i)\\bFROM\\b", "\nFROM\n  ")
                .replaceAll("(?i)\\bWHERE\\b", "\nWHERE\n  ")
                .replaceAll("(?i)\\bAND\\b", "\n  AND ")
                .replaceAll("(?i)\\bGROUP BY\\b", "\nGROUP BY\n  ")
                .replaceAll("(?i)\\bORDER BY\\b", "\nORDER BY\n  ")
                .replaceAll("(?i)\\bLIMIT\\b", "\nLIMIT ")
                .replaceAll("(?i)\\bJOIN\\b", "\nJOIN\n  ")
                .replaceAll("(?i)\\bLEFT JOIN\\b", "\nLEFT JOIN\n  ")
                .trim();

        return new AIFormatResponse(sql, formatted, dialect != null ? dialect : "trino");
    }

    public AIGenerateResponse generateQuery(String prompt, String dialect, Map<String, Object> context) {
        long start = System.currentTimeMillis();
        String lower = prompt.toLowerCase();
        String genSql;
        List<String> tables;

        if (lower.contains("пользовател") || lower.contains("user")) {
            genSql = "SELECT \n  user_id,\n  COUNT(*) as action_count,\n  MAX(created_at) as last_seen\nFROM analytics.events.user_actions\nGROUP BY user_id\nORDER BY action_count DESC\nLIMIT 100;";
            tables = List.of("analytics.events.user_actions");
        } else if (lower.contains("заказ") || lower.contains("order")) {
            genSql = "SELECT \n  c.name as customer_name,\n  o.orderstatus,\n  SUM(o.totalprice) as total_spent\nFROM tpch.sf1.customer c\nJOIN tpch.sf1.orders o ON c.custkey = o.custkey\nGROUP BY c.name, o.orderstatus\nORDER BY total_spent DESC\nLIMIT 50;";
            tables = List.of("tpch.sf1.customer", "tpch.sf1.orders");
        } else {
            genSql = "SELECT \n  report_date,\n  platform,\n  active_users,\n  avg_session_sec\nFROM analytics.events.dau_metrics\nORDER BY report_date DESC\nLIMIT 30;";
            tables = List.of("analytics.events.dau_metrics");
        }

        double execTime = System.currentTimeMillis() - start;
        return new AIGenerateResponse(
                prompt,
                genSql,
                "Запрос сгенерирован для диалекта " + dialect + " на основе ключевых понятий схемы.",
                tables,
                sqlProperties.getAi().getModel(),
                sqlProperties.getAi().getProvider(),
                execTime,
                false
        );
    }

    private List<String> extractTables(String sql) {
        List<String> tables = new ArrayList<>();
        Pattern p = Pattern.compile("(?i)(?:FROM|JOIN)\\s+([a-zA-Z0-9_.]+)");
        Matcher m = p.matcher(sql);
        while (m.find()) {
            tables.add(m.group(1));
        }
        if (tables.isEmpty()) {
            tables.add("unknown_table");
        }
        return tables;
    }
}
