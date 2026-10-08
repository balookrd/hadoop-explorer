package org.apache.hadoop.explorer.common;

import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.security.CsrfProtectionValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsrfProtectionValidatorTest {

    private final CommonSecurityProperties props;
    private final CsrfProtectionValidator validator;

    CsrfProtectionValidatorTest() {
        this.props = new CommonSecurityProperties();
        this.props.getCors().setAllowedOrigins(List.of("http://localhost:3000", "http://localhost:5173"));
        this.validator = new CsrfProtectionValidator(props);
    }

    @Test
    @DisplayName("Безопасные запросы (GET) должны проходить всегда")
    void safeMethodsShouldPass() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/v1/queues");
        assertTrue(validator.isValid(request, true));
    }

    @Test
    @DisplayName("Запросы с Bearer токеном (isCookieAuth = false) не требуют CSRF-проверок")
    void bearerAuthShouldBypassCsrf() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/queues");
        assertTrue(validator.isValid(request, false));
    }

    @Test
    @DisplayName("Должен отклонять запрос, если Sec-Fetch-Site равен cross-site")
    void shouldRejectCrossSiteFetch() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/queues");
        request.addHeader("Sec-Fetch-Site", "cross-site");
        assertFalse(validator.isValid(request, true));
    }

    @Test
    @DisplayName("Должен разрешать запрос с заголовком X-Requested-With: XMLHttpRequest")
    void shouldAllowAjaxHeader() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/queues");
        request.addHeader("X-Requested-With", "XMLHttpRequest");
        assertTrue(validator.isValid(request, true));
    }

    @Test
    @DisplayName("Должен разрешать запрос от доверенного Origin")
    void shouldAllowTrustedOrigin() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/queues");
        request.addHeader("Origin", "http://localhost:3000");
        assertTrue(validator.isValid(request, true));
    }

    @Test
    @DisplayName("Должен блокировать запрос от недоверенного Origin")
    void shouldRejectUntrustedOrigin() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/queues");
        request.addHeader("Origin", "http://malicious-site.com");
        assertFalse(validator.isValid(request, true));
    }
}
