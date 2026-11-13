package io.github.balookrd.pristan.sql.service.ai;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

/** System prompts: each asks for one JSON object whose fields map 1:1 onto the response DTO. */
public final class SqlAssistantPrompts {

    private static final String JSON_ONLY = " Ответь одним JSON-объектом без текста вне JSON. Тексты пиши на русском.";

    public static final String CHECK = "Ты проверяешь SQL-запрос для указанного диалекта на синтаксические ошибки, "
            + "риски производительности и безопасности. Схема ответа: {\"is_valid\": boolean, \"summary\": string, "
            + "\"complexity_score\": integer 1..10, \"estimated_notes\": [string], \"issues\": [{\"line\": integer, "
            + "\"column\": integer, \"end_line\": integer|null, \"end_column\": integer|null, \"severity\": "
            + "\"error\"|\"warning\"|\"info\", \"category\": \"syntax\"|\"performance\"|\"security\"|\"style\", "
            + "\"message\": string, \"rule\": string, \"suggestion\": string|null}]}." + JSON_ONLY;

    public static final String EXPLAIN = "Ты объясняешь, как движок выполнит SQL-запрос указанного диалекта. "
            + "Схема ответа: {\"explanation\": string (Markdown, шаги плана), \"summary\": string, "
            + "\"tables_used\": [string], \"operations\": [string]}." + JSON_ONLY;

    public static final String OPTIMIZE = "Ты оптимизируешь SQL-запрос указанного диалекта, не меняя результат. "
            + "Схема ответа: {\"optimized_sql\": string, \"optimizations\": [string], \"diff_summary\": string}." + JSON_ONLY;

    public static final String FIX = "Ты исправляешь SQL-запрос указанного диалекта по тексту ошибки движка. "
            + "Схема ответа: {\"fixed_sql\": string, \"explanation\": string}." + JSON_ONLY;

    public static final String GENERATE = "Ты пишешь SQL-запрос указанного диалекта по описанию на естественном языке, "
            + "используя только таблицы и колонки из контекста каталога, если он передан. Схема ответа: "
            + "{\"generated_sql\": string, \"explanation\": string, \"tables_used\": [string]}." + JSON_ONLY;

    private SqlAssistantPrompts() {
    }

    public static String userMessage(ObjectMapper mapper, String dialect, String sql, Map<String, Object> context, String extra) {
        StringBuilder message = new StringBuilder("Диалект: ").append(dialect).append('\n');
        if (sql != null) {
            message.append("SQL:\n```sql\n").append(sql).append("\n```\n");
        }
        if (extra != null) {
            message.append(extra).append('\n');
        }
        if (context != null && !context.isEmpty()) {
            try {
                message.append("Контекст каталога (JSON): ").append(mapper.writeValueAsString(context)).append('\n');
            } catch (Exception ignored) {
                // context is optional
            }
        }
        return message.toString();
    }
}
