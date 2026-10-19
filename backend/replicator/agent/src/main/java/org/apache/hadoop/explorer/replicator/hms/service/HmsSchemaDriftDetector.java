package org.apache.hadoop.explorer.replicator.hms.service;

import org.apache.hadoop.explorer.replicator.hms.model.HmsFieldSchemaDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.apache.hadoop.explorer.replicator.model.HmsSchemaDriftReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Анализатор расхождений и дрейфа схемы метаданных Hive Metastore (Schema Drift Detector).
 * Выполняет глубокое сопоставление определений таблиц, колонок, партиций и SerDe свойств.
 */
public class HmsSchemaDriftDetector {

    private static final Logger log = LoggerFactory.getLogger(HmsSchemaDriftDetector.class);

    /**
     * Сравнение схемы таблицы источника с актуальной схемой в целевом кластере.
     *
     * @param sourceTable ожидаемая/исходная таблица (с уже транслированным целевым location)
     * @param targetTable фактическая таблица в целевом Hive Metastore
     * @return структурированный отчет о дрейфе схемы
     */
    public HmsSchemaDriftReport detectDrift(HmsTableDto sourceTable, HmsTableDto targetTable) {
        if (sourceTable == null && targetTable == null) {
            return HmsSchemaDriftReport.noDrift("unknown", "unknown");
        }
        if (sourceTable == null) {
            return HmsSchemaDriftReport.withDrift(targetTable.dbName(), targetTable.tableName(),
                    List.of("Таблица отсутствует в источнике, но присутствует в таргете (Extraneous Table)"));
        }
        if (targetTable == null) {
            return HmsSchemaDriftReport.withDrift(sourceTable.dbName(), sourceTable.tableName(),
                    List.of("Таблица отсутствует в целевом кластере"));
        }

        List<String> discrepancies = new ArrayList<>();
        String tableName = sourceTable.tableName();
        String dbName = targetTable.dbName();

        // 1. Проверка типа таблицы
        if (!Objects.equals(sourceTable.tableType(), targetTable.tableType())) {
            discrepancies.add(String.format("Несовпадение типа таблицы: ожидается '%s', фактически '%s'",
                    sourceTable.tableType(), targetTable.tableType()));
        }

        // 2. Проверка колонок (схема данных)
        List<HmsFieldSchemaDto> srcCols = sourceTable.columns() != null ? sourceTable.columns() : List.of();
        List<HmsFieldSchemaDto> dstCols = targetTable.columns() != null ? targetTable.columns() : List.of();

        Map<String, String> dstColMap = dstCols.stream()
                .collect(Collectors.toMap(c -> c.name().toLowerCase(Locale.ROOT), HmsFieldSchemaDto::type, (a, b) -> a));

        Set<String> srcColNames = new HashSet<>();
        for (HmsFieldSchemaDto srcCol : srcCols) {
            String colNameLower = srcCol.name().toLowerCase(Locale.ROOT);
            srcColNames.add(colNameLower);

            String dstType = dstColMap.get(colNameLower);
            if (dstType == null) {
                discrepancies.add(String.format("Отсутствует колонка '%s' (%s) в целевой таблице",
                        srcCol.name(), srcCol.type()));
            } else if (!normalizeType(srcCol.type()).equalsIgnoreCase(normalizeType(dstType))) {
                discrepancies.add(String.format("Несовпадение типа колонки '%s': источник='%s', таргет='%s'",
                        srcCol.name(), srcCol.type(), dstType));
            }
        }

        for (HmsFieldSchemaDto dstCol : dstCols) {
            String colNameLower = dstCol.name().toLowerCase(Locale.ROOT);
            if (!srcColNames.contains(colNameLower)) {
                discrepancies.add(String.format("В целевой таблице присутствует дополнительная колонка '%s' (%s)",
                        dstCol.name(), dstCol.type()));
            }
        }

        // 3. Проверка ключей секционирования (Partition Keys)
        List<String> srcPartKeys = sourceTable.partitionKeys() != null ? sourceTable.partitionKeys() : List.of();
        List<String> dstPartKeys = targetTable.partitionKeys() != null ? targetTable.partitionKeys() : List.of();
        if (!srcPartKeys.equals(dstPartKeys)) {
            discrepancies.add(String.format("Несовпадение ключей секционирования: источник=%s, таргет=%s",
                    srcPartKeys, dstPartKeys));
        }

        // 4. Проверка SerDe библиотеки и форматов хранения
        if (sourceTable.serdeLib() != null && targetTable.serdeLib() != null
                && !sourceTable.serdeLib().equalsIgnoreCase(targetTable.serdeLib())) {
            discrepancies.add(String.format("Несовпадение SerDe библиотеки: источник='%s', таргет='%s'",
                    sourceTable.serdeLib(), targetTable.serdeLib()));
        }
        if (sourceTable.inputFormat() != null && targetTable.inputFormat() != null
                && !sourceTable.inputFormat().equalsIgnoreCase(targetTable.inputFormat())) {
            discrepancies.add(String.format("Несовпадение InputFormat: источник='%s', таргет='%s'",
                    sourceTable.inputFormat(), targetTable.inputFormat()));
        }
        if (sourceTable.outputFormat() != null && targetTable.outputFormat() != null
                && !sourceTable.outputFormat().equalsIgnoreCase(targetTable.outputFormat())) {
            discrepancies.add(String.format("Несовпадение OutputFormat: источник='%s', таргет='%s'",
                    sourceTable.outputFormat(), targetTable.outputFormat()));
        }

        if (discrepancies.isEmpty()) {
            return HmsSchemaDriftReport.noDrift(dbName, tableName);
        } else {
            log.warn("[HmsSchemaDrift] Обнаружен дрейф схемы таблицы {}.{}: {}", dbName, tableName, discrepancies);
            return HmsSchemaDriftReport.withDrift(dbName, tableName, discrepancies);
        }
    }

    private String normalizeType(String type) {
        if (type == null) return "";
        return type.replaceAll("\\s+", "").toLowerCase(Locale.ROOT);
    }
}
