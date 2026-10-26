package org.apache.hadoop.explorer.replicator.hms.service;

import org.apache.hadoop.explorer.replicator.hms.client.MockHmsClient;
import org.apache.hadoop.explorer.replicator.hms.model.HmsNotificationEventDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Общее окно журнала событий: метастор читается один раз на несколько задач.
 */
@DisplayName("HMS: общее окно журнала событий")
class NotificationLogCacheTest {

    /** Метастор, считающий чтения журнала. */
    private static final class CountingHms extends MockHmsClient {
        final AtomicInteger reads = new AtomicInteger();

        final long base;

        CountingHms() {
            super("dc1", "HDP_3.1");
            base = getCurrentNotificationEventId();
        }

        @Override
        public List<HmsNotificationEventDto> getNextNotifications(long lastEventId, int maxEvents) {
            reads.incrementAndGet();
            return super.getNextNotifications(lastEventId, maxEvents);
        }
    }

    private static CountingHms hmsWithEvents(int count) {
        CountingHms hms = new CountingHms();
        for (int i = 0; i < count; i++) {
            hms.emitEvent("CREATE_TABLE", "db" + (i % 10), "t" + i, "{}");
        }
        return hms;
    }

    @Test
    @DisplayName("Десять задач на одной позиции читают метастор один раз")
    void jobsAtTheSamePositionShareOneRead() {
        CountingHms hms = hmsWithEvents(500);
        NotificationLogCache cache = new NotificationLogCache(hms, 10_000, 1000, System::currentTimeMillis);

        long base = hms.base;
        for (int job = 0; job < 10; job++) {
            assertEquals(500, cache.next(base, 1000).size());
        }

        assertEquals(1, hms.reads.get(), "журнал читается один раз на все задачи");
    }

    @Test
    @DisplayName("Догнавшие метастор задачи опрашивают хвост не чаще интервала")
    void tailIsPolledAtMostOncePerInterval() {
        CountingHms hms = hmsWithEvents(5);
        AtomicLong now = new AtomicLong(0);
        NotificationLogCache cache = new NotificationLogCache(hms, 10_000, 1000, now::get);
        long base = hms.base;
        cache.next(base, 1000);
        int readsAfterCatchUp = hms.reads.get();

        for (int job = 0; job < 10; job++) {
            assertEquals(0, cache.next(base + 5, 1000).size());
        }
        assertEquals(readsAfterCatchUp + 1, hms.reads.get(), "хвост проверен один раз");

        hms.emitEvent("CREATE_TABLE", "db1", "fresh", "{}");
        now.addAndGet(1000);
        assertEquals(1, cache.next(base + 5, 1000).size(), "после интервала новое событие видно");
    }

    @Test
    @DisplayName("Разрыв журнала не скрывается окном: задача видит первое доступное событие")
    void gapIsNotHiddenByTheWindow() {
        CountingHms hms = hmsWithEvents(10);
        NotificationLogCache cache = new NotificationLogCache(hms, 10_000, 0, System::currentTimeMillis);
        long base = hms.base;
        cache.next(base + 5, 1000);
        hms.dropEventsUpTo(base + 8);

        // Задача на позиции base+2: в окне есть только события после base+5, а base+3..base+8
        // метастор уже удалил — окно не должно склеить участок, задача видит разрыв
        List<HmsNotificationEventDto> events = cache.next(base + 2, 1000);

        assertEquals(base + 9, events.get(0).eventId(), "первое доступное событие после разрыва");
    }
}
