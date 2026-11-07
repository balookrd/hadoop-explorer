package org.apache.hadoop.explorer.common;

import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.resilience.RateLimiterService;
import org.apache.hadoop.explorer.common.security.ClientIpResolver;
import org.apache.hadoop.explorer.common.security.RateLimitFilter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


class RateLimitMetricsTest {

    private static RateLimiterService limiter() {
        CommonSecurityProperties props = new CommonSecurityProperties();
        props.getRateLimiter().setEnabled(true);
        props.getRateLimiter().setBurstCapacity(1);
        props.getRateLimiter().setRequestsPerMinute(1);
        props.getRateLimiter().getLogin().setBurstCapacity(1);
        props.getRateLimiter().getLogin().setRequestsPerMinute(1);
        return new RateLimiterService(props);
    }

    private static int send(RateLimitFilter filter, String method, String uri) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest(method, uri);
        request.setRemoteAddr("192.0.2.1");
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        return response.getStatus();
    }

    @Test
    @DisplayName("Отказ уровня API и уровня входа сообщается слушателю с нужным tier")
    void blockedRequestsAreReportedPerTier() throws Exception {
        Map<RateLimiterService.Tier, Integer> blocks = new EnumMap<>(RateLimiterService.Tier.class);
        RateLimitFilter filter = new RateLimitFilter(limiter(), new ClientIpResolver(List.of()),
            tier -> blocks.merge(tier, 1, Integer::sum));

        assertEquals(200, send(filter, "GET", "/api/v1/x"));
        assertEquals(429, send(filter, "GET", "/api/v1/x"));
        assertEquals(429, send(filter, "GET", "/api/v1/x"));
        assertEquals(200, send(filter, "POST", "/api/v1/auth/login"));
        assertEquals(429, send(filter, "POST", "/api/v1/auth/login"));

        assertEquals(2, blocks.get(RateLimiterService.Tier.API));
        assertEquals(1, blocks.get(RateLimiterService.Tier.LOGIN));
    }

    @Test
    @DisplayName("Пропущенный запрос слушателя не вызывает")
    void allowedRequestDoesNotNotifyListener() throws Exception {
        Map<RateLimiterService.Tier, Integer> blocks = new EnumMap<>(RateLimiterService.Tier.class);
        RateLimitFilter filter = new RateLimitFilter(limiter(), new ClientIpResolver(List.of()),
            tier -> blocks.merge(tier, 1, Integer::sum));

        assertEquals(200, send(filter, "GET", "/api/v1/x"));

        assertEquals(0, blocks.size());
    }

    @Test
    @DisplayName("Без слушателя фильтр по-прежнему отвечает 429")
    void worksWithoutListener() throws Exception {
        RateLimitFilter filter = new RateLimitFilter(limiter(), new ClientIpResolver(List.of()));

        assertEquals(200, send(filter, "GET", "/api/v1/x"));
        assertEquals(429, send(filter, "GET", "/api/v1/x"));
    }
}
