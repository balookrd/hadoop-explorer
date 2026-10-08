package org.apache.hadoop.explorer.common.security;

import org.apache.hadoop.explorer.common.model.UserSession;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.Collection;
import java.util.List;

/**
 * Аутентификационный токен Spring Security для сессий Hadoop Explorer.
 */
public class CommonAuthenticationToken implements Authentication {

    private final UserSession userSession;
    private final List<GrantedAuthority> authorities;
    private boolean authenticated = true;

    public CommonAuthenticationToken(UserSession userSession) {
        this.userSession = userSession;
        this.authorities = List.of(
            new SimpleGrantedAuthority("ROLE_" + userSession.systemRole().name())
        );
    }

    public UserSession getUserSession() {
        return userSession;
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return authorities;
    }

    @Override
    public Object getCredentials() {
        return "";
    }

    @Override
    public Object getDetails() {
        return userSession;
    }

    @Override
    public Object getPrincipal() {
        return userSession;
    }

    @Override
    public boolean isAuthenticated() {
        return authenticated;
    }

    @Override
    public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException {
        this.authenticated = isAuthenticated;
    }

    @Override
    public String getName() {
        return userSession.username();
    }
}
