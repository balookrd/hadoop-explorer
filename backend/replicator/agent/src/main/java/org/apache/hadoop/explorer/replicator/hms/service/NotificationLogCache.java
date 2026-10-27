package org.apache.hadoop.explorer.replicator.hms.service;

import org.apache.hadoop.explorer.replicator.hms.client.HmsClient;
import org.apache.hadoop.explorer.replicator.hms.model.HmsNotificationEventDto;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * Общее окно журнала событий метастора для всех CDC-задач агента.
 *
 * <p>Журнал NOTIFICATION_LOG у метастора один на все базы, а фильтра по базе у
 * {@code get_next_notification} в Hive 3.1 нет. Каждая задача (одна на базу) читала весь журнал
 * сама и отбрасывала чужие события: при десятках задач на нагруженном метасторе он вычитывался
 * десятки раз. Окно держит последние прочитанные события, и задачи на близких позициях
 * получают их из одного чтения.
 *
 * <p>Из окна отдается только непрерывный участок, начинающийся ровно со следующего события:
 * разрыв журнала (события удалены по TTL) задача должна увидеть сама, как и при прямом чтении.
 * Задачи, догнавшие метастор, опрашивают хвост журнала не чаще {@code tailIntervalMs}.
 *
 * <p>Окно защищено {@link ReentrantLock}, а не {@code synchronized}: CDC-задачи исполняются в
 * виртуальных потоках, и в Java 21 ожидание входа в монитор прикрепляет поток-носитель. Десятки
 * задач, ждущих окно, занимали все носители, и поток, которому метастор уже ответил, не мог
 * вернуть соединение в пул — читающий журнал ждал его по 30 с, CDC стоял.
 */
final class NotificationLogCache {

    private final HmsClient hmsClient;
    private final int maxEvents;
    private final long tailIntervalMs;
    private final LongSupplier clock;

    private final ReentrantLock lock = new ReentrantLock();
    private final NavigableMap<Long, HmsNotificationEventDto> window = new TreeMap<>();
    private long tailCheckedAtMs = Long.MIN_VALUE;
    private long tailCheckedAfterId = -1;
    private long currentIdCachedAtMs;
    private long currentIdCached;
    /**
     * Номер еще не запрашивался. Отдельный флаг, а не {@code Long.MIN_VALUE} во времени запроса:
     * {@code now - Long.MIN_VALUE} переполнялось в отрицательное, номер не запрашивался ни разу,
     * и агент рапортовал нулевое отставание CDC при любом отставании.
     */
    private boolean currentIdKnown;

    NotificationLogCache(HmsClient hmsClient, int maxEvents, long tailIntervalMs, LongSupplier clock) {
        this.hmsClient = hmsClient;
        this.maxEvents = Math.max(1, maxEvents);
        this.tailIntervalMs = tailIntervalMs;
        this.clock = clock;
    }

    /** События после {@code afterId}, не больше {@code limit}. */
    List<HmsNotificationEventDto> next(long afterId, int limit) {
        lock.lock();
        try {
            return nextLocked(afterId, limit);
        } finally {
            lock.unlock();
        }
    }

    private List<HmsNotificationEventDto> nextLocked(long afterId, int limit) {
        List<HmsNotificationEventDto> fromWindow = contiguousFrom(afterId, limit);
        if (fromWindow.size() >= limit) {
            return fromWindow;
        }
        long readAfter = fromWindow.isEmpty() ? afterId : fromWindow.get(fromWindow.size() - 1).eventId();
        boolean atTail = !window.isEmpty() && readAfter >= window.lastKey();
        long now = clock.getAsLong();
        if (atTail && !fromWindow.isEmpty()) {
            // Часть отдана из окна, а дальше — хвост: новое дочитается следующим опросом
            return fromWindow;
        }
        if ((atTail || window.isEmpty()) && readAfter == tailCheckedAfterId && now - tailCheckedAtMs < tailIntervalMs) {
            // Хвост только что проверяли: новых событий не было, метастор не дергаем
            return fromWindow;
        }

        List<HmsNotificationEventDto> fetched = hmsClient.getNextNotifications(readAfter, limit - fromWindow.size());
        if (fetched.isEmpty()) {
            tailCheckedAtMs = now;
            tailCheckedAfterId = readAfter;
        }
        for (HmsNotificationEventDto e : fetched) {
            window.put(e.eventId(), e);
        }
        while (window.size() > maxEvents) {
            window.pollFirstEntry();
        }
        List<HmsNotificationEventDto> result = new ArrayList<>(fromWindow);
        result.addAll(fetched);
        return result;
    }

    /** Текущий номер события метастора, не чаще одного запроса за {@code tailIntervalMs}. */
    long currentEventId() {
        lock.lock();
        try {
            long now = clock.getAsLong();
            if (!currentIdKnown || now - currentIdCachedAtMs >= tailIntervalMs) {
                currentIdCached = hmsClient.getCurrentNotificationEventId();
                currentIdCachedAtMs = now;
                currentIdKnown = true;
            }
            return currentIdCached;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Текущий номер события прямо из метастора, мимо кэша. Если он меньше событий в окне,
     * журнал начался заново (метастор восстановлен из резервной копии или пересоздан), и окно
     * со старыми событиями сбрасывается: иначе оно отдавало бы их задачам как новые.
     */
    long refreshCurrentEventId() {
        lock.lock();
        try {
            currentIdCached = hmsClient.getCurrentNotificationEventId();
            currentIdCachedAtMs = clock.getAsLong();
            currentIdKnown = true;
            if (!window.isEmpty() && window.lastKey() > currentIdCached) {
                window.clear();
                tailCheckedAtMs = Long.MIN_VALUE;
                tailCheckedAfterId = -1;
            }
            return currentIdCached;
        } finally {
            lock.unlock();
        }
    }

    private List<HmsNotificationEventDto> contiguousFrom(long afterId, int limit) {
        List<HmsNotificationEventDto> out = new ArrayList<>();
        long expected = afterId + 1;
        for (HmsNotificationEventDto e : window.tailMap(afterId, false).values()) {
            if (e.eventId() != expected || out.size() >= limit) {
                break;
            }
            out.add(e);
            expected++;
        }
        return out;
    }
}
