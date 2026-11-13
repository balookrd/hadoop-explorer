package io.github.balookrd.pristan.replicator.orchestrator.scheduler;

import org.springframework.scheduling.support.CronExpression;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parsed job schedule: a fixed interval ({@code @every_<N>s|m|h}, legacy {@code every_<N>s}) or a
 * calendar expression evaluated by Spring {@link CronExpression}.
 *
 * <p>Calendar input is a standard 5-field cron line (minute, hour, day of month, month, day of
 * week) or a macro. Spring needs a seconds field, so the parser prepends {@code 0}. When both day
 * fields are restricted, Spring requires both to match (Vixie cron matches either).
 */
public final class CronSchedule {

    /** Shortest interval between runs: the scheduler polls every 5 seconds. */
    static final Duration MIN_INTERVAL = Duration.ofSeconds(5);

    private static final Pattern EVERY_MACRO = Pattern.compile("^@every_(\\d{1,9})([smh])$");
    private static final Pattern LEGACY_EVERY_SECONDS = Pattern.compile("^every_(\\d{1,9})s$");
    private static final Set<String> SPRING_MACROS =
        Set.of("@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly");

    private final Duration interval;
    private final CronExpression cron;
    private final String source;

    private CronSchedule(Duration interval, CronExpression cron, String source) {
        this.interval = interval;
        this.cron = cron;
        this.source = source;
    }

    public static CronSchedule parse(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new InvalidCronExpressionException(expression, "выражение не задано");
        }
        String expr = expression.trim();
        String lower = expr.toLowerCase(Locale.ROOT);

        Matcher every = EVERY_MACRO.matcher(lower);
        if (every.matches()) {
            return interval(expression, Long.parseLong(every.group(1)), every.group(2).charAt(0));
        }
        Matcher legacy = LEGACY_EVERY_SECONDS.matcher(lower);
        if (legacy.matches()) {
            return interval(expression, Long.parseLong(legacy.group(1)), 's');
        }
        if (lower.equals("@minutely")) {
            return calendar(expression, "0 * * * * *");
        }
        if (lower.startsWith("@")) {
            if (!SPRING_MACROS.contains(lower)) {
                throw new InvalidCronExpressionException(expression, "неизвестный макрос; допустимы "
                    + "@every_<N>s|m|h, @minutely, @hourly, @daily, @midnight, @weekly, @monthly, @yearly, @annually");
            }
            return calendar(expression, lower);
        }
        String[] fields = expr.split("\\s+");
        if (fields.length != 5) {
            throw new InvalidCronExpressionException(expression,
                "ожидается 5 полей (минута, час, день месяца, месяц, день недели), получено " + fields.length);
        }
        return calendar(expression, "0 " + String.join(" ", fields));
    }

    private static CronSchedule interval(String expression, long amount, char unit) {
        if (amount <= 0) {
            throw new InvalidCronExpressionException(expression, "интервал должен быть больше нуля");
        }
        Duration duration = switch (unit) {
            case 's' -> Duration.ofSeconds(amount);
            case 'm' -> Duration.ofMinutes(amount);
            default -> Duration.ofHours(amount);
        };
        return new CronSchedule(duration.compareTo(MIN_INTERVAL) < 0 ? MIN_INTERVAL : duration, null, expression);
    }

    private static CronSchedule calendar(String expression, String springExpression) {
        CronExpression parsed;
        try {
            parsed = CronExpression.parse(springExpression);
        } catch (IllegalArgumentException e) {
            throw new InvalidCronExpressionException(expression, e.getMessage());
        }
        if (parsed.next(ZonedDateTime.now(ZoneOffset.UTC)) == null) {
            throw new InvalidCronExpressionException(expression, "выражение никогда не срабатывает");
        }
        return new CronSchedule(null, parsed, expression);
    }

    public boolean isCalendar() {
        return cron != null;
    }

    public Instant next(Instant after, ZoneId zone) {
        if (cron == null) {
            return after.plus(interval);
        }
        ZonedDateTime next = cron.next(after.atZone(zone));
        if (next == null) {
            throw new InvalidCronExpressionException(source, "выражение больше не срабатывает");
        }
        return next.toInstant();
    }
}
