package io.github.balookrd.pristan.parom.orchestrator.throttler;

import io.github.balookrd.pristan.parom.orchestrator.config.ParomProperties;
import io.github.balookrd.pristan.parom.orchestrator.topology.TopologyRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Нулевой лимит канала означает запрет, а не отсутствие лимита.
 *
 * <p>Сетевое ограждение Kill-Switch ставит лимит канала в ноль, но в ведре
 * {@code limitBytesPerSec <= 0} трактовалось как «лимита нет»: расчет ожидания сразу
 * возвращал ноль. То есть оператор нажимал Kill-Switch, интерфейс показывал
 * {@code isFenced=true}, а агенты продолжали качать данные через WAN на полной скорости и
 * вообще без шейпинга.
 */
@DisplayName("Токен-бакет: нулевой лимит перекрывает канал")
class ZeroLimitBlocksTrafficTest {

    private ParomProperties properties;
    private TopologyRegistry topologyRegistry;
    private TokenBucketThrottler throttler;

    @BeforeEach
    void setUp() {
        properties = new ParomProperties();
        properties.setGlobalLimitBytesPerSec(100L * 1024 * 1024);
        topologyRegistry = new TopologyRegistry(properties);
        throttler = new TokenBucketThrottler(properties, topologyRegistry);
    }

    /** Ожидание, которое означает «канал перекрыт», а не «подожди немного». */
    private static final double BLOCKING_WAIT_SECONDS = 30.0;

    @Test
    @DisplayName("Канал с нулевым лимитом не пропускает трафик")
    void testZeroLimitBlocksTraffic() {
        topologyRegistry.setDcLimit("dc1", "dc2", 0L);

        double wait = throttler.requestTokens(64 * 1024, "dc1", "dc2");

        assertTrue(wait >= BLOCKING_WAIT_SECONDS,
                "При перекрытом канале ожидание обязано быть запретительным, получено " + wait
                        + " с: иначе ограждение Kill-Switch фиктивно, а UI показывает isFenced=true");
    }

    @Test
    @DisplayName("Канал без заданного лимита продолжает работать")
    void testAbsentLimitDoesNotBlock() {
        double wait = throttler.requestTokens(64 * 1024, "dc1", "dc2");

        assertTrue(wait < BLOCKING_WAIT_SECONDS,
                "Отсутствие лимита канала не должно останавливать репликацию, получено " + wait + " с");
    }

    @Test
    @DisplayName("Положительный лимит пропускает трафик")
    void testPositiveLimitDoesNotBlock() {
        topologyRegistry.setDcLimit("dc1", "dc2", 10L * 1024 * 1024);

        double wait = throttler.requestTokens(64 * 1024, "dc1", "dc2");

        assertTrue(wait < BLOCKING_WAIT_SECONDS, "Получено ожидание " + wait + " с");
    }

    @Test
    @DisplayName("Снятие ограждения возвращает трафик")
    void testUnfencingRestoresTraffic() {
        topologyRegistry.setDcLimit("dc1", "dc2", 0L);
        assertTrue(throttler.requestTokens(64 * 1024, "dc1", "dc2") >= BLOCKING_WAIT_SECONDS);

        topologyRegistry.setDcLimit("dc1", "dc2", 50L * 1024 * 1024);

        assertTrue(throttler.requestTokens(64 * 1024, "dc1", "dc2") < BLOCKING_WAIT_SECONDS,
                "После снятия ограждения канал должен снова пропускать трафик");
    }

    @Test
    @DisplayName("Перекрытый канал отвечает признаком blocked")
    void testZeroLimitIsReportedAsBlocked() {
        topologyRegistry.setDcLimit("dc1", "dc2", 0L);

        assertTrue(throttler.acquireTokens(64 * 1024, "dc1", "dc2").blocked(),
                "Агент отличает запрет от обычного ожидания только по этому признаку");
    }

    @Test
    @DisplayName("Положительный лимит не отвечает признаком blocked")
    void testPositiveLimitIsNotReportedAsBlocked() {
        topologyRegistry.setDcLimit("dc1", "dc2", 10L * 1024 * 1024);

        assertFalse(throttler.acquireTokens(64 * 1024, "dc1", "dc2").blocked());
    }

    @Test
    @DisplayName("Нулевой глобальный лимит отвечает признаком blocked")
    void testZeroGlobalLimitIsReportedAsBlocked() {
        throttler.setGlobalLimit(0L);

        assertTrue(throttler.acquireTokens(64 * 1024, "dc1", "dc2").blocked());
    }

    @Test
    @DisplayName("Запросы в перекрытый канал не списывают токены с остальных уровней")
    void testBlockedRequestsChargeNothing() {
        topologyRegistry.setDcLimit("dc1", "dc2", 0L);
        // Агенты переспрашивают, пока канал перекрыт: 10 запросов по половине глобальной корзины
        for (int i = 0; i < 10; i++) {
            throttler.acquireTokens(50L * 1024 * 1024, "dc1", "dc2");
        }

        topologyRegistry.setDcLimit("dc1", "dc2", 1024L * 1024 * 1024);

        // Корзины канала после разблокировки пусты, поэтому ожидание не строго нулевое: 64 КБ при
        // 600 МБ/с — доли миллисекунды. Долг в глобальной корзине дал бы секунды.
        double wait = throttler.acquireTokens(64 * 1024, "dc1", "dc2").waitSeconds();
        assertTrue(wait < 0.05,
                "После снятия ограждения трафик не должен ждать погашения долга, набранного отказами: " + wait + " с");
    }
}
