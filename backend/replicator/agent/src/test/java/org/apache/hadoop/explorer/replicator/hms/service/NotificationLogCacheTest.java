package org.apache.hadoop.explorer.replicator.hms.service;

import org.apache.hadoop.explorer.replicator.hms.client.MockHmsClient;
import org.apache.hadoop.explorer.replicator.hms.model.HmsNotificationEventDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    @DisplayName("Номер события метастора запрашивается с первого вызова и обновляется раз в интервал")
    void currentEventIdIsFetchedAndRefreshed() {
        CountingHms hms = hmsWithEvents(3);
        AtomicLong now = new AtomicLong(0);
        NotificationLogCache cache = new NotificationLogCache(hms, 10_000, 1000, now::get);

        assertEquals(hms.base + 3, cache.currentEventId(), "без первого запроса отставание считалось бы от нуля");
        hms.emitEvent("CREATE_TABLE", "db1", "t", "{}");
        assertEquals(hms.base + 3, cache.currentEventId(), "в пределах интервала — из кэша");
        now.addAndGet(1000);
        assertEquals(hms.base + 4, cache.currentEventId());
    }

    @Test
    @DisplayName("Журнал начался заново: свежий номер сбрасывает окно со старыми событиями")
    void metastoreResetClearsWindow() {
        CountingHms hms = hmsWithEvents(10);
        NotificationLogCache cache = new NotificationLogCache(hms, 10_000, 60_000, System::currentTimeMillis);
        long base = hms.base;
        cache.next(base, 1000);
        cache.currentEventId();

        hms.resetNotificationLog(base);
        hms.emitEvent("CREATE_TABLE", "db1", "after_reset", "{}");

        assertEquals(base + 10, cache.currentEventId(), "кэш номера еще помнит старый журнал");
        assertEquals(base + 1, cache.refreshCurrentEventId());
        List<HmsNotificationEventDto> events = cache.next(base, 1000);
        assertEquals(1, events.size());
        assertEquals("after_reset", events.get(0).tableName(), "окно не должно отдавать события старого журнала");
    }

    /** Метастор, чтение журнала которого стоит, пока его не отпустят. */
    private static final class BlockingHms extends MockHmsClient {
        final CountDownLatch insideRead = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        BlockingHms() {
            super("dc1", "HDP_3.1");
        }

        @Override
        public List<HmsNotificationEventDto> getNextNotifications(long lastEventId, int maxEvents) {
            insideRead.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return super.getNextNotifications(lastEventId, maxEvents);
        }
    }

    /**
     * CDC-задачи агента исполняются в виртуальных потоках. Пока окно было {@code synchronized},
     * задача, ждущая входа в монитор, в Java 21 прикрепляла к себе поток-носитель. Десятки задач
     * занимали все носители, и поток, которому метастор уже ответил, не мог продолжить и вернуть
     * соединение в пул: читающий журнал внутри монитора ждал соединение 30 с (стенд CDC под
     * нагрузкой: 30 задач на 6 ядрах стояли, CPU агента — ноль).
     */
    @Test
    @DisplayName("Задачи, ждущие окно журнала, не занимают потоки-носители")
    void waitingJobsDoNotPinCarrierThreads() throws Exception {
        BlockingHms hms = new BlockingHms();
        NotificationLogCache cache = new NotificationLogCache(hms, 10_000, 0, System::currentTimeMillis);

        List<Thread> threads = new ArrayList<>();
        threads.add(Thread.ofVirtual().start(() -> cache.next(0, 100)));
        assertTrue(hms.insideRead.await(5, TimeUnit.SECONDS), "чтение журнала не началось");
        int contenders = 4 * Runtime.getRuntime().availableProcessors();
        for (int i = 0; i < contenders; i++) {
            threads.add(Thread.ofVirtual().start(() -> cache.next(0, 100)));
        }
        Thread.sleep(300);

        // Ответ метастора обрабатывает такой же виртуальный поток: ему нужен свободный носитель
        threads.add(Thread.ofVirtual().start(hms.release::countDown));
        boolean released = hms.release.await(10, TimeUnit.SECONDS);
        hms.release.countDown();
        for (Thread t : threads) {
            t.join(5_000);
        }

        assertTrue(released, "все потоки-носители заняты задачами, ждущими окно журнала");
    }
}
