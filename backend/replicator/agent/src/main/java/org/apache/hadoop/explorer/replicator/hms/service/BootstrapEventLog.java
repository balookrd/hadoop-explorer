package org.apache.hadoop.explorer.replicator.hms.service;

import org.apache.hadoop.explorer.replicator.model.HmsEventReportDto;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Журнал событий bootstrap, который отправляется оркестратору порциями.
 *
 * <p>На таблице в 200k партиций, лежащих вне корня таблицы, событие пишется на каждую
 * партицию. Одним итоговым отчетом это десятки мегабайт: оркестратор отвергнет его по лимиту
 * gRPC-сообщения, статус ACTIVE не сохранится, и bootstrap пойдет по кругу. Поэтому журнал
 * копит события до {@code flushThreshold} и сразу отдает их промежуточным отчетом, а в
 * итоговый попадает только хвост.
 *
 * <p>Наличие ошибок запоминается отдельно: события с ними к концу bootstrap могут быть уже
 * отправлены, и просмотр оставшегося хвоста их не покажет.
 */
final class BootstrapEventLog extends AbstractList<HmsEventReportDto> {

    /** Порция событий в одном отчете: ~1 КБ на событие держит сообщение в пределах мегабайта. */
    static final int DEFAULT_FLUSH_THRESHOLD = 500;

    private final int flushThreshold;
    private final Consumer<List<HmsEventReportDto>> flusher;
    private final List<HmsEventReportDto> pending = new ArrayList<>();
    private boolean failed;

    BootstrapEventLog(int flushThreshold, Consumer<List<HmsEventReportDto>> flusher) {
        this.flushThreshold = Math.max(1, flushThreshold);
        this.flusher = flusher;
    }

    @Override
    public boolean add(HmsEventReportDto event) {
        List<HmsEventReportDto> batch = null;
        synchronized (this) {
            pending.add(event);
            if ("FAILED".equalsIgnoreCase(event.status())) {
                failed = true;
            }
            if (pending.size() >= flushThreshold) {
                batch = new ArrayList<>(pending);
                pending.clear();
            }
        }
        // Отправка вне блокировки: соседние таблицы не ждут сетевого вызова
        if (batch != null) {
            flusher.accept(batch);
        }
        return true;
    }

    @Override
    public synchronized HmsEventReportDto get(int index) {
        return pending.get(index);
    }

    @Override
    public synchronized int size() {
        return pending.size();
    }

    synchronized boolean hasFailures() {
        return failed;
    }

    /** Неотправленный хвост для итогового отчета. */
    synchronized List<HmsEventReportDto> drain() {
        List<HmsEventReportDto> rest = new ArrayList<>(pending);
        pending.clear();
        return rest;
    }
}
