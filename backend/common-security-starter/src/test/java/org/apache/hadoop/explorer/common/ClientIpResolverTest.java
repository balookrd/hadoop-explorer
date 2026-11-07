package org.apache.hadoop.explorer.common;

import org.apache.hadoop.explorer.common.security.ClientIpResolver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientIpResolverTest {

    private static MockHttpServletRequest request(String peer, String forwardedFor) {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setRemoteAddr(peer);
        if (forwardedFor != null) {
            request.addHeader("X-Forwarded-For", forwardedFor);
        }
        return request;
    }

    @Test
    @DisplayName("Без доверенных прокси X-Forwarded-For игнорируется")
    void ignoresForwardedForWithoutTrustedProxies() {
        ClientIpResolver resolver = new ClientIpResolver(List.of());
        assertEquals("203.0.113.7", resolver.resolve(request("203.0.113.7", "198.51.100.1")));
    }

    @Test
    @DisplayName("От недоверенного адреса X-Forwarded-For игнорируется")
    void ignoresForwardedForFromUntrustedPeer() {
        ClientIpResolver resolver = new ClientIpResolver(List.of("10.0.0.0/8"));
        assertEquals("203.0.113.7", resolver.resolve(request("203.0.113.7", "198.51.100.1")));
    }

    @Test
    @DisplayName("От доверенного прокси клиент — крайний справа недоверенный адрес")
    void returnsRightmostUntrustedHop() {
        ClientIpResolver resolver = new ClientIpResolver(List.of("10.0.0.0/8"));
        assertEquals("198.51.100.23",
            resolver.resolve(request("10.0.0.5", "6.6.6.6, 198.51.100.23, 10.1.2.3")));
    }

    @Test
    @DisplayName("Если все адреса доверенные, клиент — крайний слева")
    void returnsLeftmostWhenAllHopsTrusted() {
        ClientIpResolver resolver = new ClientIpResolver(List.of("10.0.0.0/8"));
        assertEquals("10.9.9.9", resolver.resolve(request("10.0.0.5", "10.9.9.9, 10.1.1.1")));
    }

    @Test
    @DisplayName("Не-IP значение в цепочке останавливает разбор")
    void stopsAtMalformedHop() {
        ClientIpResolver resolver = new ClientIpResolver(List.of("10.0.0.0/8"));
        assertEquals("10.0.0.5", resolver.resolve(request("10.0.0.5", "198.51.100.1, not-an-ip")));
    }

    @Test
    @DisplayName("IPv6: доверенная подсеть и адрес клиента")
    void supportsIpv6() {
        ClientIpResolver resolver = new ClientIpResolver(List.of("fd00::/8"));
        assertEquals("2001:db8::42", resolver.resolve(request("fd00::1", "2001:db8::42")));
    }

    @Test
    @DisplayName("Имя хоста в trusted-proxies — ошибка конфигурации")
    void rejectsHostNameInConfig() {
        assertThrows(IllegalArgumentException.class, () -> new ClientIpResolver(List.of("proxy.corp.local")));
    }

    @Test
    @DisplayName("Несколько строк X-Forwarded-For склеиваются по порядку: подделка в первой строке не берётся")
    void joinsMultipleForwardedForHeaderLines() {
        ClientIpResolver resolver = new ClientIpResolver(List.of("10.0.0.0/8"));
        MockHttpServletRequest request = request("10.0.0.5", "1.2.3.4");
        request.addHeader("X-Forwarded-For", "198.51.100.23");
        assertEquals("198.51.100.23", resolver.resolve(request));
    }
}
