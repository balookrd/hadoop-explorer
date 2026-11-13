package io.github.balookrd.pristan.spark.service.engine.livy;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SPNEGO (RFC 4559) client for Livy with livy.server.auth.type = kerberos: keytab login through
 * Krb5LoginModule, a fresh SPNEGO token for HTTP/&lt;livy host&gt; per request. No Hadoop UGI involved.
 * <p>
 * The keytab login is cached: a new login happens after relogin-interval or at 80 % of the TGT lifetime,
 * whichever comes first, so an expired TGT never reaches Livy. While one thread logs in again, the others keep
 * using the previous Subject if its TGT is still valid instead of waiting for the KDC. A failed relogin with a
 * still valid TGT is logged and retried after RELOGIN_BACKOFF; the error reaches the caller only when there
 * is no valid Subject at all (first login, or the TGT has expired).
 * <p>
 * The JAAS configuration is private to this provider (passed to LoginContext), the global JAAS Configuration
 * and java.security.krb5.conf are left untouched: krb5.conf comes from -Djava.security.krb5.conf.
 * Subject.callAs works on Java 21 (delegates to doAs) and on newer JDKs where GSS reads Subject.current().
 */
public final class SpnegoAuthHeaderProvider implements LivyAuthHeaderProvider {

    private static final String SPNEGO_OID = "1.3.6.1.5.5.2";
    /** Share of the TGT lifetime after which the service logs in again (as Hadoop UGI does). */
    private static final double TGT_RENEW_WINDOW = 0.8;
    /** Delay before the next login attempt after a failed relogin while the previous TGT is still valid. */
    static final Duration RELOGIN_BACKOFF = Duration.ofMinutes(1);

    private static final Logger log = LoggerFactory.getLogger(SpnegoAuthHeaderProvider.class);

    private final String principal;
    private final String keytab;
    private final Duration reloginInterval;
    private final Clock clock;
    private final ReentrantLock loginLock = new ReentrantLock();
    private final AtomicInteger loginAttempts = new AtomicInteger();
    /** Subject and the times derived from its TGT, replaced together on a successful login. */
    private volatile Login current;
    private volatile Instant reloginAt = Instant.EPOCH;

    private record Login(Subject subject, Instant tgtEnd, Instant reloginAt) {
        boolean validAt(Instant now) {
            return tgtEnd != null && now.isBefore(tgtEnd);
        }
    }

    public SpnegoAuthHeaderProvider(String principal, String keytab, Duration reloginInterval) {
        this(principal, keytab, reloginInterval, Clock.systemUTC());
    }

    SpnegoAuthHeaderProvider(String principal, String keytab, Duration reloginInterval, Clock clock) {
        if (principal == null || principal.isBlank() || keytab == null || keytab.isBlank()) {
            throw new IllegalStateException("livy.auth=spnego требует livy.principal и livy.keytab");
        }
        if (reloginInterval == null || reloginInterval.isZero() || reloginInterval.isNegative()) {
            throw new IllegalStateException("livy.relogin-interval-minutes должен быть больше 0");
        }
        this.principal = principal;
        this.keytab = keytab;
        this.reloginInterval = reloginInterval;
        this.clock = clock;
    }

    /** Number of keytab logins started so far, failed ones included. */
    int loginAttempts() {
        return loginAttempts.get();
    }

    @Override
    public Optional<String> authorizationHeader(URI target) {
        Subject current = currentSubject();
        try {
            byte[] token = Subject.callAs(current, () -> initToken(target.getHost()));
            return Optional.of("Negotiate " + Base64.getEncoder().encodeToString(token));
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            // The exception text reaches API responses and execution history: the principal stays in the log
            log.warn("SPNEGO: не удалось получить билет для HTTP/{} от имени {}: {}", target.getHost(), principal,
                    cause.getMessage());
            throw new LivyException(401, "SPNEGO: не удалось получить билет для HTTP/" + target.getHost()
                    + " (livy.principal): " + hideCredentials(cause.getMessage()), cause);
        }
    }

    private static byte[] initToken(String host) throws GSSException {
        GSSManager manager = GSSManager.getInstance();
        GSSName server = manager.createName("HTTP@" + host, GSSName.NT_HOSTBASED_SERVICE);
        GSSContext context = manager.createContext(server, new Oid(SPNEGO_OID), null, GSSContext.DEFAULT_LIFETIME);
        try {
            context.requestMutualAuth(false);
            context.requestCredDeleg(false);
            return context.initSecContext(new byte[0], 0, 0);
        } finally {
            context.dispose();
        }
    }

    private Subject currentSubject() {
        Login known = current;
        Instant now = clock.instant();
        if (known != null && now.isBefore(reloginAt)) {
            return known.subject();
        }
        boolean usable = known != null && known.validAt(now);
        if (usable) {
            // Another thread is already logging in: keep the still valid TGT instead of waiting for the KDC
            if (!loginLock.tryLock()) {
                return known.subject();
            }
        } else {
            loginLock.lock();
        }
        try {
            Login latest = current;
            now = clock.instant();
            if (latest != null && now.isBefore(reloginAt)) {
                return latest.subject();
            }
            try {
                Login fresh = login(now);
                current = fresh;
                reloginAt = fresh.reloginAt();
                return fresh.subject();
            } catch (LivyException e) {
                now = clock.instant();
                if (latest == null || !latest.validAt(now)) {
                    throw e;
                }
                Duration remaining = Duration.between(now, latest.tgtEnd());
                Duration retryIn = remaining.compareTo(RELOGIN_BACKOFF) < 0 ? remaining : RELOGIN_BACKOFF;
                reloginAt = now.plus(retryIn);
                log.warn("Повторный Kerberos-вход {} не выполнен, используется прежний TGT (действует до {}), "
                        + "следующая попытка через {} с: {}", principal, latest.tgtEnd(), retryIn.toSeconds(), e.getMessage());
                return latest.subject();
            }
        } finally {
            loginLock.unlock();
        }
    }

    private Login login(Instant loginTime) {
        loginAttempts.incrementAndGet();
        Subject fresh = login();
        KerberosTicket tgt = tgt(fresh);
        Instant tgtStart = tgt == null || tgt.getStartTime() == null ? null : tgt.getStartTime().toInstant();
        Instant tgtEnd = tgt == null || tgt.getEndTime() == null ? null : tgt.getEndTime().toInstant();
        return new Login(fresh, tgtEnd, reloginDeadline(loginTime, reloginInterval, tgtStart, tgtEnd));
    }

    /** The earlier of loginTime + interval and the point at 80 % of the TGT lifetime. */
    static Instant reloginDeadline(Instant loginTime, Duration interval, Instant tgtStart, Instant tgtEnd) {
        Instant byInterval = loginTime.plus(interval);
        if (tgtStart == null || tgtEnd == null || !tgtEnd.isAfter(tgtStart)) {
            return byInterval;
        }
        long lifetimeMillis = Duration.between(tgtStart, tgtEnd).toMillis();
        Instant byTicket = tgtStart.plusMillis((long) (lifetimeMillis * TGT_RENEW_WINDOW));
        return byTicket.isBefore(byInterval) ? byTicket : byInterval;
    }

    private static KerberosTicket tgt(Subject subject) {
        for (KerberosTicket ticket : subject.getPrivateCredentials(KerberosTicket.class)) {
            KerberosPrincipal server = ticket.getServer();
            if (server != null && server.getName().startsWith("krbtgt/")) {
                return ticket;
            }
        }
        return null;
    }

    private Subject login() {
        Map<String, String> options = Map.of(
                "useKeyTab", "true",
                "keyTab", keytab,
                "principal", principal,
                "storeKey", "true",
                "doNotPrompt", "true",
                "isInitiator", "true",
                // Re-reads the krb5.conf named by -Djava.security.krb5.conf (or the JDK default) on every login,
                // so an edited file (new KDC list, realm mapping) is picked up at the next relogin without a
                // restart. Nothing is set: the JVM-wide Kerberos configuration is only reloaded from the same file
                "refreshKrb5Config", "true");
        Configuration config = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[]{new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)};
            }
        };
        try {
            LoginContext context = new LoginContext("hadoop-explorer-livy", new Subject(), null, config);
            context.login();
            return context.getSubject();
        } catch (LoginException e) {
            // The exception text reaches API responses and execution history: principal and keytab path stay in the log
            log.error("Kerberos-вход {} по keytab {} не выполнен: {}", principal, keytab, e.getMessage());
            throw new LivyException(401, "Kerberos-вход сервиса по keytab не выполнен (livy.principal, livy.keytab): "
                    + hideCredentials(e.getMessage()), e);
        }
    }

    /** JAAS and GSS texts may name the principal or the keytab file; both are replaced with ***. */
    private String hideCredentials(String text) {
        if (text == null) {
            return "";
        }
        return text.replace(keytab, "***").replace(principal, "***");
    }
}
