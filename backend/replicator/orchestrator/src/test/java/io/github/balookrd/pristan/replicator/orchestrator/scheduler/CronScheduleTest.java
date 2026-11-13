package io.github.balookrd.pristan.replicator.orchestrator.scheduler;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class CronScheduleTest {

    // Thursday
    private static final Instant BASE = Instant.parse("2026-10-08T12:00:00Z");

    private static Instant next(String expression) {
        return CronSchedule.parse(expression).next(BASE, ZoneOffset.UTC);
    }

    @Test
    @DisplayName("Интервальные макросы отсчитываются от предыдущего запуска")
    void intervalMacros() {
        assertEquals(BASE.plus(Duration.ofMinutes(5)), next("@every_5m"));
        assertEquals(BASE.plus(Duration.ofMinutes(15)), next("@every_15m"));
        assertEquals(BASE.plus(Duration.ofHours(2)), next("@every_2h"));
        assertEquals(BASE.plusSeconds(30), next("every_30s"));
        assertEquals(BASE.plusSeconds(5), next("every_1s"), "интервал меньше 5 секунд поднимается до 5");
        assertFalse(CronSchedule.parse("@every_5m").isCalendar());
    }

    @Test
    @DisplayName("Календарные макросы привязаны к календарю, а не к прошлому запуску")
    void calendarMacros() {
        assertEquals(Instant.parse("2026-10-08T12:01:00Z"), next("@minutely"));
        assertEquals(Instant.parse("2026-10-08T13:00:00Z"), next("@hourly"));
        assertEquals(Instant.parse("2026-10-09T00:00:00Z"), next("@daily"));
        assertEquals(Instant.parse("2026-10-09T00:00:00Z"), next("@midnight"));
        assertEquals(Instant.parse("2026-10-11T00:00:00Z"), next("@weekly"));
        assertEquals(Instant.parse("2026-11-01T00:00:00Z"), next("@monthly"));
        assertEquals(Instant.parse("2027-01-01T00:00:00Z"), next("@yearly"));
        assertTrue(CronSchedule.parse("@daily").isCalendar());
    }

    @Test
    @DisplayName("Пять полей cron: полночь — это полночь, */10 — границы десятков минут")
    void fiveFieldExpressions() {
        assertEquals(Instant.parse("2026-10-09T00:00:00Z"), next("0 0 * * *"));
        assertEquals(Instant.parse("2026-10-08T12:10:00Z"),
            CronSchedule.parse("*/10 * * * *").next(Instant.parse("2026-10-08T12:03:20Z"), ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Календарное выражение считается в заданном часовом поясе")
    void honoursTimeZone() {
        // 15:00 MSK Thursday -> Friday 02:30 MSK
        assertEquals(Instant.parse("2026-10-08T23:30:00Z"),
            CronSchedule.parse("30 2 * * 1-5").next(BASE, ZoneId.of("Europe/Moscow")));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {
        "   ", "every tuesday", "@fortnightly", "0 0 * *", "0 0 0 * * *",
        "61 * * * *", "0 0 30 2 *", "@every_0m", "@every_99999999999m"
    })
    @DisplayName("Невалидные выражения отклоняются")
    void rejectsInvalidExpressions(String expression) {
        InvalidCronExpressionException e =
            assertThrows(InvalidCronExpressionException.class, () -> CronSchedule.parse(expression));
        assertTrue(e.getMessage().startsWith("Некорректное cron-выражение"), e.getMessage());
    }
}
