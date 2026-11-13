package io.github.balookrd.pristan.sql.service.engine.jdbc;

import io.github.balookrd.pristan.sql.config.SqlProperties;

/** The account the service itself authenticates as; end users are impersonated on top of it. */
public final class ServiceUser {

    public static final String DEFAULT = "hadoop-explorer";

    private ServiceUser() {
    }

    /** jdbc.service-user, else the short name of kerberos.principal, else DEFAULT. */
    public static String resolve(SqlProperties.ClusterConfig cluster) {
        String configured = cluster.getJdbc().getServiceUser();
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        SqlProperties.KerberosConfig kerberos = cluster.getKerberos();
        if (kerberos.isEnabled() && kerberos.getPrincipal() != null && !kerberos.getPrincipal().isBlank()) {
            return shortName(kerberos.getPrincipal());
        }
        return DEFAULT;
    }

    /** svc/host@REALM -> svc. Trino maps the principal the same way with user-mapping.pattern=(.*)@.* plus host stripping. */
    static String shortName(String principal) {
        int cut = principal.length();
        int slash = principal.indexOf('/');
        int at = principal.indexOf('@');
        if (slash >= 0) cut = Math.min(cut, slash);
        if (at >= 0) cut = Math.min(cut, at);
        return principal.substring(0, cut);
    }
}
