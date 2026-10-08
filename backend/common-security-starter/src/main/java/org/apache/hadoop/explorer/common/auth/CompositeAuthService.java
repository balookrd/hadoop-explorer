package org.apache.hadoop.explorer.common.auth;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.common.model.LoginRequest;
import org.apache.hadoop.explorer.common.model.TokenResponse;
import org.apache.hadoop.explorer.common.model.UserSession;
import org.apache.hadoop.explorer.common.security.CommonAuthenticationToken;
import org.apache.hadoop.explorer.common.security.JwtTokenService;
import org.apache.hadoop.explorer.common.security.SecurityTokenExtractor;
import org.apache.hadoop.explorer.common.session.SessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseCookie;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Композитный сервис авторизации: координирует LDAP, Kerberos, Mock,
 * установку HttpOnly Cookies и персистентных сессий.
 */
public class CompositeAuthService implements AuthService {

    private static final Logger log = LoggerFactory.getLogger(CompositeAuthService.class);

    private final CommonSecurityProperties properties;
    private final JwtTokenService jwtTokenService;
    private final SessionStore sessionStore;
    private final SecurityTokenExtractor tokenExtractor;
    private final LdapAuthService ldapAuthService;
    private final KerberosSpnegoService kerberosSpnegoService;
    private final MockAuthService mockAuthService;

    public CompositeAuthService(
        CommonSecurityProperties properties,
        JwtTokenService jwtTokenService,
        SessionStore sessionStore,
        SecurityTokenExtractor tokenExtractor,
        LdapAuthService ldapAuthService,
        KerberosSpnegoService kerberosSpnegoService,
        MockAuthService mockAuthService
    ) {
        this.properties = properties;
        this.jwtTokenService = jwtTokenService;
        this.sessionStore = sessionStore;
        this.tokenExtractor = tokenExtractor;
        this.ldapAuthService = ldapAuthService;
        this.kerberosSpnegoService = kerberosSpnegoService;
        this.mockAuthService = mockAuthService;
    }

    @Override
    public TokenResponse login(LoginRequest request, HttpServletRequest httpRequest, HttpServletResponse httpResponse) {
        String mode = properties.getAuth().getMode();
        Optional<UserSession> sessionOpt = Optional.empty();

        if ("ldap".equalsIgnoreCase(mode)) {
            sessionOpt = ldapAuthService.authenticate(request.username(), request.password());
        } else if ("mock".equalsIgnoreCase(mode)) {
            sessionOpt = mockAuthService.authenticate(request.username(), request.password());
        } else {
            // Если Kerberos или неизвестно, пробуем сначала LDAP, затем fallback
            sessionOpt = ldapAuthService.authenticate(request.username(), request.password());
        }

        if (sessionOpt.isEmpty()) {
            throw new BadCredentialsException("Неверное имя пользователя или пароль");
        }

        UserSession session = sessionOpt.get();
        return processSuccessfulAuth(session, httpResponse);
    }

    @Override
    public TokenResponse sso(HttpServletRequest httpRequest, HttpServletResponse httpResponse) {
        Optional<UserSession> sessionOpt = kerberosSpnegoService.authenticate(httpRequest);
        if (sessionOpt.isEmpty()) {
            throw new BadCredentialsException("Kerberos SPNEGO аутентификация не удалась");
        }

        return processSuccessfulAuth(sessionOpt.get(), httpResponse);
    }

    private TokenResponse processSuccessfulAuth(UserSession session, HttpServletResponse httpResponse) {
        String token = jwtTokenService.createToken(session);
        int expMinutes = properties.getJwt().getExpirationMinutes();
        Instant expInstant = Instant.now().plusSeconds((long) expMinutes * 60);

        // Сохраняем сессию
        sessionStore.saveSession(token, session, expInstant, null);

        // Устанавливаем Cookie в браузер
        attachAuthCookies(token, expMinutes, httpResponse);

        return TokenResponse.of(token, session);
    }

    private void attachAuthCookies(String token, int expMinutes, HttpServletResponse response) {
        var cookieProps = properties.getCookie();
        for (String cookieName : cookieProps.getNames()) {
            ResponseCookie.ResponseCookieBuilder builder = ResponseCookie.from(cookieName, token)
                .httpOnly(cookieProps.isHttpOnly())
                .secure(cookieProps.isSecure())
                .path(cookieProps.getPath())
                .maxAge(Duration.ofMinutes(expMinutes))
                .sameSite(cookieProps.getSameSite());

            if (cookieProps.getDomain() != null && !cookieProps.getDomain().isBlank()) {
                builder.domain(cookieProps.getDomain());
            }

            response.addHeader(HttpHeaders.SET_COOKIE, builder.build().toString());
        }
    }

    @Override
    public void logout(HttpServletRequest httpRequest, HttpServletResponse httpResponse) {
        var extracted = tokenExtractor.extract(httpRequest);
        if (extracted.token() != null) {
            sessionStore.deleteSession(extracted.token());
            sessionStore.revokeToken(extracted.token(), getCurrentUser(httpRequest).map(UserSession::username).orElse("unknown"), null);
        }

        // Очищаем Cookie в ответе
        var cookieProps = properties.getCookie();
        for (String cookieName : cookieProps.getNames()) {
            ResponseCookie.ResponseCookieBuilder builder = ResponseCookie.from(cookieName, "")
                .httpOnly(cookieProps.isHttpOnly())
                .secure(cookieProps.isSecure())
                .path(cookieProps.getPath())
                .maxAge(0)
                .sameSite(cookieProps.getSameSite());

            if (cookieProps.getDomain() != null && !cookieProps.getDomain().isBlank()) {
                builder.domain(cookieProps.getDomain());
            }

            httpResponse.addHeader(HttpHeaders.SET_COOKIE, builder.build().toString());
        }

        SecurityContextHolder.clearContext();
    }

    @Override
    public Optional<UserSession> getCurrentUser(HttpServletRequest httpRequest) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth instanceof CommonAuthenticationToken tokenAuth) {
            return Optional.of(tokenAuth.getUserSession());
        }

        var extracted = tokenExtractor.extract(httpRequest);
        if (extracted.token() != null) {
            return sessionStore.getSession(extracted.token());
        }

        return Optional.empty();
    }
}
