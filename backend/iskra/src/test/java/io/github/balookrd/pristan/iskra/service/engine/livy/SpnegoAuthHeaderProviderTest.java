package io.github.balookrd.pristan.iskra.service.engine.livy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpnegoAuthHeaderProviderTest {

    private static final String REALM = "EXAMPLE.COM";
    private static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";
    private static final URI LIVY = URI.create("http://localhost:8998/sessions");

    @TempDir
    static Path workDir;

    private static SimpleKdcServer kdc;
    private static Path clientKeytab;
    private static Path serverKeytab;
    private static String previousKrb5Conf;
    private static Level previousKerbyLevel;

    @BeforeAll
    static void startKdc() throws Exception {
        // The KDC logs every AS/TGS exchange and the expected "principal is not known" at INFO/ERROR
        Logger kerbyLog = (Logger) LoggerFactory.getLogger("org.apache.kerby");
        previousKerbyLevel = kerbyLog.getLevel();
        kerbyLog.setLevel(Level.OFF);
        kdc = new SimpleKdcServer();
        kdc.setWorkDir(workDir.toFile());
        kdc.setKdcHost("localhost");
        kdc.setKdcRealm(REALM);
        kdc.setAllowUdp(false);
        kdc.setKdcTcpPort(freePort());
        kdc.init();   // writes <workDir>/krb5.conf
        kdc.start();
        clientKeytab = workDir.resolve("svc.keytab");
        serverKeytab = workDir.resolve("http.keytab");
        kdc.createAndExportPrincipals(clientKeytab.toFile(), "svc_iskra");
        kdc.createAndExportPrincipals(serverKeytab.toFile(), "HTTP/localhost");
        // The JDK Kerberos configuration is process-wide: the previous value is restored after the class, and
        // every login here uses refreshKrb5Config=true, so no other test sees this KDC's krb5.conf
        previousKrb5Conf = System.getProperty(KRB5_CONF_PROPERTY);
        System.setProperty(KRB5_CONF_PROPERTY, workDir.resolve("krb5.conf").toString());
    }

    @AfterAll
    static void stopKdc() throws Exception {
        if (previousKrb5Conf == null) {
            System.clearProperty(KRB5_CONF_PROPERTY);
        } else {
            System.setProperty(KRB5_CONF_PROPERTY, previousKrb5Conf);
        }
        if (kdc != null) {
            kdc.stop();
        }
        ((Logger) LoggerFactory.getLogger("org.apache.kerby")).setLevel(previousKerbyLevel);
    }

    @Test
    void producesTokenAcceptedByTheHttpService() throws Exception {
        SpnegoAuthHeaderProvider provider = new SpnegoAuthHeaderProvider(
                "svc_iskra@" + REALM, clientKeytab.toString(), Duration.ofHours(1));

        String header = provider.authorizationHeader(LIVY).orElseThrow();

        assertTrue(header.startsWith("Negotiate "));
        byte[] token = Base64.getDecoder().decode(header.substring("Negotiate ".length()));
        assertEquals("svc_iskra@" + REALM, acceptAsHttpService(token));
    }

    @Test
    void keytabLoginIsReusedAcrossRequests() throws Exception {
        Path keytab = Files.copy(clientKeytab, workDir.resolve("reused.keytab"));
        SpnegoAuthHeaderProvider provider = new SpnegoAuthHeaderProvider(
                "svc_iskra@" + REALM, keytab.toString(), Duration.ofHours(1));
        provider.authorizationHeader(LIVY).orElseThrow();

        // Without a cached login the second request would fail: the keytab is gone
        Files.delete(keytab);
        String header = provider.authorizationHeader(LIVY).orElseThrow();

        byte[] token = Base64.getDecoder().decode(header.substring("Negotiate ".length()));
        assertEquals("svc_iskra@" + REALM, acceptAsHttpService(token));
    }

    @Test
    void failedReloginKeepsTheStillValidTicket() throws Exception {
        Path keytab = Files.copy(clientKeytab, workDir.resolve("relogin.keytab"));
        SpnegoAuthHeaderProvider provider = new SpnegoAuthHeaderProvider(
                "svc_iskra@" + REALM, keytab.toString(), Duration.ofMillis(1));
        provider.authorizationHeader(LIVY).orElseThrow();
        Thread.sleep(5);

        // Relogin is due but fails (as with an unreachable KDC): the TGT of the first login still works
        Files.delete(keytab);
        String header = provider.authorizationHeader(LIVY).orElseThrow();

        byte[] token = Base64.getDecoder().decode(header.substring("Negotiate ".length()));
        assertEquals("svc_iskra@" + REALM, acceptAsHttpService(token));
        assertEquals(2, provider.loginAttempts());
        // The next attempt waits for RELOGIN_BACKOFF instead of hitting the KDC on every request
        provider.authorizationHeader(LIVY).orElseThrow();
        assertEquals(2, provider.loginAttempts());
    }

    @Test
    void failedReloginAfterTheTicketExpiredIsReported() throws Exception {
        Path keytab = Files.copy(clientKeytab, workDir.resolve("expired.keytab"));
        MutableClock clock = new MutableClock();
        SpnegoAuthHeaderProvider provider = new SpnegoAuthHeaderProvider(
                "svc_iskra@" + REALM, keytab.toString(), Duration.ofHours(1), clock);
        provider.authorizationHeader(LIVY).orElseThrow();

        Files.delete(keytab);
        clock.advance(Duration.ofDays(30));
        LivyException e = assertThrows(LivyException.class, () -> provider.authorizationHeader(LIVY));
        assertEquals(401, e.getStatus());
    }

    @Test
    void concurrentRequestsLogInOnceWhenReloginIsDue() throws Exception {
        MutableClock clock = new MutableClock();
        SpnegoAuthHeaderProvider provider = new SpnegoAuthHeaderProvider(
                "svc_iskra@" + REALM, clientKeytab.toString(), Duration.ofMinutes(1), clock);
        provider.authorizationHeader(LIVY).orElseThrow();
        clock.advance(Duration.ofMinutes(2));

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<String>> results = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                results.add(pool.submit(() -> {
                    start.await();
                    return provider.authorizationHeader(LIVY).orElseThrow();
                }));
            }
            start.countDown();
            for (Future<String> result : results) {
                assertTrue(result.get(30, TimeUnit.SECONDS).startsWith("Negotiate "));
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(2, provider.loginAttempts());
    }

    @Test
    void missingKeytabIsReportedAsLivyException() {
        SpnegoAuthHeaderProvider provider = new SpnegoAuthHeaderProvider(
                "svc_iskra@" + REALM, workDir.resolve("missing.keytab").toString(), Duration.ofHours(1));

        LivyException e = assertThrows(LivyException.class, () -> provider.authorizationHeader(LIVY));
        assertEquals(401, e.getStatus());
        // The message reaches the API (detail) and execution history: principal and keytab path stay in the log
        assertFalse(e.getMessage().contains("svc_iskra"), e.getMessage());
        assertFalse(e.getMessage().contains("missing.keytab"), e.getMessage());
        assertTrue(e.getMessage().contains("livy.keytab"), e.getMessage());
        assertFalse(e.getMessage().contains("Negotiate"), e.getMessage());
    }

    @Test
    void unknownServicePrincipalIsReportedAsLivyException() {
        SpnegoAuthHeaderProvider provider = new SpnegoAuthHeaderProvider(
                "svc_iskra@" + REALM, clientKeytab.toString(), Duration.ofHours(1));

        LivyException e = assertThrows(LivyException.class,
                () -> provider.authorizationHeader(URI.create("http://127.0.0.1:8998/sessions")));
        assertEquals(401, e.getStatus());
        assertTrue(e.getMessage().contains("HTTP/127.0.0.1"), e.getMessage());
        assertFalse(e.getMessage().contains("svc_iskra"), e.getMessage());
    }

    @Test
    void principalAndKeytabAreRequired() {
        assertThrows(IllegalStateException.class, () -> new SpnegoAuthHeaderProvider(null, "/k", Duration.ofHours(1)));
        assertThrows(IllegalStateException.class, () -> new SpnegoAuthHeaderProvider("svc@" + REALM, " ", Duration.ofHours(1)));
        assertThrows(IllegalStateException.class, () -> new SpnegoAuthHeaderProvider("svc@" + REALM, "/k", Duration.ZERO));
    }

    @Test
    void reloginHappensBeforeTheTicketExpires() {
        Instant login = Instant.parse("2026-01-01T00:00:00Z");
        // TGT of 10 hours: the configured interval comes first
        assertEquals(login.plus(Duration.ofHours(1)), SpnegoAuthHeaderProvider.reloginDeadline(
                login, Duration.ofHours(1), login, login.plus(Duration.ofHours(10))));
        // TGT of 30 minutes: relogin at 80 % of its lifetime, before the interval
        assertEquals(login.plus(Duration.ofMinutes(24)), SpnegoAuthHeaderProvider.reloginDeadline(
                login, Duration.ofHours(1), login, login.plus(Duration.ofMinutes(30))));
        // No TGT information: only the interval
        assertEquals(login.plus(Duration.ofHours(1)), SpnegoAuthHeaderProvider.reloginDeadline(
                login, Duration.ofHours(1), null, null));
    }

    /** Server side of SPNEGO, as Livy's AuthenticationFilter does it. */
    private static String acceptAsHttpService(byte[] token) throws Exception {
        Map<String, String> options = Map.of("useKeyTab", "true", "keyTab", serverKeytab.toString(),
                "principal", "HTTP/localhost@" + REALM, "storeKey", "true", "doNotPrompt", "true",
                "isInitiator", "false", "refreshKrb5Config", "true");
        Configuration config = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[]{new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)};
            }
        };
        LoginContext login = new LoginContext("http-acceptor", new Subject(), null, config);
        login.login();
        return Subject.callAs(login.getSubject(), () -> {
            GSSManager manager = GSSManager.getInstance();
            Oid spnego = new Oid("1.3.6.1.5.5.2");
            GSSCredential credential = manager.createCredential(null, GSSCredential.INDEFINITE_LIFETIME, spnego, GSSCredential.ACCEPT_ONLY);
            GSSContext context = manager.createContext(credential);
            context.acceptSecContext(token, 0, token.length);
            assertTrue(context.isEstablished());
            return context.getSrcName().toString();
        });
    }

    /** Clock the test moves forward; the TGT itself keeps its real KDC times. */
    private static final class MutableClock extends Clock {
        private final AtomicReference<Instant> now = new AtomicReference<>(Instant.now());

        void advance(Duration duration) {
            now.updateAndGet(t -> t.plus(duration));
        }

        @Override
        public Instant instant() {
            return now.get();
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
