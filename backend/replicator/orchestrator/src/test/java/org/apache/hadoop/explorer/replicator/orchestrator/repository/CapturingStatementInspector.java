package org.apache.hadoop.explorer.replicator.orchestrator.repository;

import org.hibernate.resource.jdbc.spi.StatementInspector;

import java.util.ArrayList;
import java.util.List;

/**
 * Перехватчик SQL для тестов: собирает запросы текущего потока.
 *
 * <p>Глобальная статистика Hibernate непригодна для точных утверждений в полном прогоне —
 * фоновые планировщики оркестратора добавляют в нее свои операции. Перехват в
 * {@link ThreadLocal} видит только запросы проверяемого вызова.
 */
public class CapturingStatementInspector implements StatementInspector {

    private static final ThreadLocal<List<String>> CAPTURED = new ThreadLocal<>();

    public static void start() {
        CAPTURED.set(new ArrayList<>());
    }

    public static List<String> stop() {
        List<String> captured = CAPTURED.get();
        CAPTURED.remove();
        return captured != null ? captured : List.of();
    }

    public static long countMatching(List<String> statements, String fragment) {
        return statements.stream()
                .filter(sql -> sql.toLowerCase().contains(fragment.toLowerCase()))
                .count();
    }

    @Override
    public String inspect(String sql) {
        List<String> captured = CAPTURED.get();
        if (captured != null) {
            captured.add(sql);
        }
        return sql;
    }
}
