package io.github.balookrd.pristan.sql.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balookrd.pristan.sql.config.SqlProperties;
import io.github.balookrd.pristan.sql.dto.*;
import io.github.balookrd.pristan.sql.service.ai.LlmUnavailableException;
import io.github.balookrd.pristan.sql.service.ai.OpenAiCompatibleClient;
import io.github.balookrd.pristan.sql.service.ai.SqlAssistantPrompts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class AiService {

    static final String RULES_PROVIDER = "mock-builtin";
    static final String RULES_MODEL = "builtin-rules";
    static final String LLM_PROVIDER = "openai_compatible";

    private static final int MAX_POSITION = 1_000_000;
    private static final Set<String> SEVERITIES = Set.of("error", "warning", "info");
    private static final Set<String> CATEGORIES = Set.of("syntax", "performance", "security", "style");

    private static final Logger log = LoggerFactory.getLogger(AiService.class);

    private final SqlProperties sqlProperties;
    private final ObjectMapper objectMapper;
    private final OpenAiCompatibleClient llm; // null: rules only

    public AiService(SqlProperties sqlProperties, ObjectMapper objectMapper) {
        this.sqlProperties = sqlProperties;
        this.objectMapper = objectMapper;
        SqlProperties.AiConfig ai = sqlProperties.getAi();
        this.llm = ai.isEnabled() && LLM_PROVIDER.equalsIgnoreCase(ai.getProvider())
                ? new OpenAiCompatibleClient(ai, objectMapper)
                : null;
    }

    public AIStatusResponse getStatus() {
        var ai = sqlProperties.getAi();
        String baseUrl = OpenAiCompatibleClient.displayUrl(ai.getUrl());
        if (llm == null) {
            return new AIStatusResponse(ai.isEnabled(), RULES_PROVIDER, RULES_MODEL, baseUrl, false,
                    "LLM не подключена (sql.ai.provider: " + ai.getProvider() + (ai.isEnabled() ? "" : ", sql.ai.enabled: false")
                            + "), ответы строятся встроенными правилами", null);
        }
        try {
            double latencyMs = llm.ping();
            return new AIStatusResponse(true, ai.getProvider(), ai.getModel(), baseUrl, true,
                    "LLM " + ai.getModel() + " отвечает", latencyMs);
        } catch (LlmUnavailableException e) {
            log.warn("Проверка LLM для /api/v1/ai/status не прошла: {}", e.getMessage());
            // Р8: while the model is down the answers come from the rules, the status says the same
            return new AIStatusResponse(true, RULES_PROVIDER, RULES_MODEL, baseUrl, false,
                    "LLM " + ai.getModel() + " недоступна (" + e.getSummary() + "), ответы строятся встроенными правилами", null);
        }
    }

    public AICheckResponse checkQuery(String sql, String dialect, Map<String, Object> context) {
        if (llm == null) {
            return rulesCheck(sql, dialect, false);
        }
        long start = System.nanoTime();
        try {
            JsonNode r = llm.completeJson(SqlAssistantPrompts.CHECK, SqlAssistantPrompts.userMessage(objectMapper, dialect, sql, context, null));
            List<AIIssue> issues = new ArrayList<>();
            for (JsonNode n : r.path("issues")) {
                issues.add(new AIIssue(
                        position(n.path("line").asInt(1)),
                        position(n.path("column").asInt(1)),
                        n.hasNonNull("end_line") ? position(n.get("end_line").asInt()) : null,
                        n.hasNonNull("end_column") ? position(n.get("end_column").asInt()) : null,
                        oneOf(n.path("severity").asText(""), SEVERITIES, "warning"),
                        oneOf(n.path("category").asText(""), CATEGORIES, "style"),
                        n.path("message").asText(""),
                        n.path("rule").asText("llm"),
                        n.hasNonNull("suggestion") ? n.get("suggestion").asText() : null));
            }
            boolean valid = r.has("is_valid")
                    ? r.get("is_valid").asBoolean()
                    : issues.stream().noneMatch(i -> "error".equalsIgnoreCase(i.severity()));
            int score = Math.max(1, Math.min(10, r.path("complexity_score").asInt(1)));
            return new AICheckResponse(valid, issues, r.path("summary").asText(""), score, complexityLevel(score),
                    texts(r.path("estimated_notes")), llmModel(), llmProvider(), elapsedMs(start), false);
        } catch (LlmUnavailableException e) {
            logFallback("check", e);
            return rulesCheck(sql, dialect, true);
        }
    }

    public AIExplainResponse explainQuery(String sql, String dialect) {
        if (llm == null) {
            return rulesExplain(sql, dialect);
        }
        long start = System.nanoTime();
        try {
            JsonNode r = llm.completeJson(SqlAssistantPrompts.EXPLAIN, SqlAssistantPrompts.userMessage(objectMapper, dialect, sql, null, null));
            return new AIExplainResponse(requireText(r, "explanation"), r.path("summary").asText(""),
                    texts(r.path("tables_used")), texts(r.path("operations")), llmModel(), llmProvider(), elapsedMs(start));
        } catch (LlmUnavailableException e) {
            logFallback("explain", e);
            return rulesExplain(sql, dialect);
        }
    }

    public AIOptimizeResponse optimizeQuery(String sql, String dialect, Map<String, Object> context) {
        if (llm == null) {
            return rulesOptimize(sql, dialect);
        }
        long start = System.nanoTime();
        try {
            JsonNode r = llm.completeJson(SqlAssistantPrompts.OPTIMIZE, SqlAssistantPrompts.userMessage(objectMapper, dialect, sql, context, null));
            return new AIOptimizeResponse(sql, requireText(r, "optimized_sql"), texts(r.path("optimizations")),
                    r.path("diff_summary").asText(""), llmModel(), llmProvider(), elapsedMs(start));
        } catch (LlmUnavailableException e) {
            logFallback("optimize", e);
            return rulesOptimize(sql, dialect);
        }
    }

    public AIFixResponse fixQuery(String sql, String dialect, String rawErrorMessage) {
        String errorMessage = rawErrorMessage == null ? "" : rawErrorMessage;
        if (llm == null) {
            return rulesFix(sql, dialect, errorMessage);
        }
        long start = System.nanoTime();
        try {
            JsonNode r = llm.completeJson(SqlAssistantPrompts.FIX,
                    SqlAssistantPrompts.userMessage(objectMapper, dialect, sql, null, errorMessage.isBlank() ? null : "Ошибка движка: " + errorMessage));
            return new AIFixResponse(sql, requireText(r, "fixed_sql"), r.path("explanation").asText(""),
                    llmModel(), llmProvider(), elapsedMs(start));
        } catch (LlmUnavailableException e) {
            logFallback("fix", e);
            return rulesFix(sql, dialect, errorMessage);
        }
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
        if (llm == null) {
            return rulesGenerate(prompt, dialect, false);
        }
        long start = System.nanoTime();
        try {
            JsonNode r = llm.completeJson(SqlAssistantPrompts.GENERATE,
                    SqlAssistantPrompts.userMessage(objectMapper, dialect, null, context, "Описание: " + prompt));
            return new AIGenerateResponse(prompt, requireText(r, "generated_sql"), r.path("explanation").asText(""),
                    texts(r.path("tables_used")), llmModel(), llmProvider(), elapsedMs(start), false);
        } catch (LlmUnavailableException e) {
            logFallback("generate", e);
            return rulesGenerate(prompt, dialect, true);
        }
    }

    // ---- Built-in rules (used when no LLM is configured and as the fallback) ----

    private AICheckResponse rulesCheck(String sql, String dialect, boolean fallbackUsed) {
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

        double execTime = System.currentTimeMillis() - start;
        return new AICheckResponse(
                isValid,
                issues,
                isValid ? "Синтаксис корректен. Проблем безопасности не обнаружено." : "Обнаружены потенциальные ошибки в запросе.",
                score,
                complexityLevel(score),
                List.of("Синтаксис проверен для диалекта: " + dialect),
                RULES_MODEL,
                RULES_PROVIDER,
                execTime,
                fallbackUsed
        );
    }

    private AIExplainResponse rulesExplain(String sql, String dialect) {
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
                RULES_MODEL,
                RULES_PROVIDER,
                execTime
        );
    }

    private AIOptimizeResponse rulesOptimize(String sql, String dialect) {
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
                RULES_MODEL,
                RULES_PROVIDER,
                execTime
        );
    }

    private AIFixResponse rulesFix(String sql, String dialect, String errorMessage) {
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
                RULES_MODEL,
                RULES_PROVIDER,
                execTime
        );
    }

    private AIGenerateResponse rulesGenerate(String prompt, String dialect, boolean fallbackUsed) {
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
                RULES_MODEL,
                RULES_PROVIDER,
                execTime,
                fallbackUsed
        );
    }

    // ---- helpers ----

    private String llmModel() {
        return sqlProperties.getAi().getModel();
    }

    private static String llmProvider() {
        return LLM_PROVIDER;
    }

    /** Model positions are 1-based; nonsense values are clamped so the editor markers stay in range. */
    private static int position(int value) {
        return Math.max(1, Math.min(MAX_POSITION, value));
    }

    private static String oneOf(String value, Set<String> allowed, String fallback) {
        String normalized = value.strip().toLowerCase(Locale.ROOT);
        return allowed.contains(normalized) ? normalized : fallback;
    }

    private static String complexityLevel(int score) {
        return score <= 2 ? "Низкая" : (score <= 5 ? "Средняя" : "Высокая");
    }

    private static List<String> texts(JsonNode array) {
        List<String> values = new ArrayList<>();
        array.forEach(node -> values.add(node.asText()));
        return values;
    }

    private static String requireText(JsonNode node, String field) throws LlmUnavailableException {
        String value = node.path(field).asText("");
        if (value.isBlank()) {
            throw new LlmUnavailableException("в ответе модели нет поля " + field);
        }
        return value;
    }

    private static double elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000.0;
    }

    private void logFallback(String operation, LlmUnavailableException e) {
        log.warn("LLM недоступна для {}, ответ построен встроенными правилами: {}", operation, e.getMessage());
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
