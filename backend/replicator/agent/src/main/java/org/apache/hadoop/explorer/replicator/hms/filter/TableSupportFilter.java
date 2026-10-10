package org.apache.hadoop.explorer.replicator.hms.filter;

import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;

import java.util.*;

public class TableSupportFilter {

    private static final Set<String> REMOVED_VENDOR_PARAMS = Set.of(
            "hdp.version",
            "STATS_GENERATED_VIA_STATS_TASK",
            "numFilesErasureCoded"
    );

    public record FilterResult(boolean supported, String reason) {
        public static FilterResult accept() {
            return new FilterResult(true, "OK");
        }
        public static FilterResult skippedAcid(String reason) {
            return new FilterResult(false, reason);
        }
    }

    /**
     * Валидация поддержки репликации таблицы:
     * - Разрешены External таблицы
     * - Разрешены Managed Non-Transactional таблицы
     * - Запрещены Full ACID / Transactional таблицы (transactional = true)
     */
    public FilterResult evaluate(HmsTableDto table) {
        if (table == null) {
            return FilterResult.skippedAcid("Таблица не найдена");
        }

        Map<String, String> params = table.parameters();
        if (params != null) {
            String transactional = params.get("transactional");
            if ("true".equalsIgnoreCase(transactional)) {
                return FilterResult.skippedAcid("ACID таблицы не поддерживаются (параметр transactional=true)");
            }
        }

        return FilterResult.accept();
    }

    /**
     * Очистка вендорных параметров HDP 3.1 перед передачей в Apache Hive 3.1.3.
     */
    public Map<String, String> sanitizeParameters(Map<String, String> originalParams) {
        if (originalParams == null || originalParams.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> cleaned = new HashMap<>(originalParams);
        for (String key : REMOVED_VENDOR_PARAMS) {
            cleaned.remove(key);
        }
        return cleaned;
    }
}
