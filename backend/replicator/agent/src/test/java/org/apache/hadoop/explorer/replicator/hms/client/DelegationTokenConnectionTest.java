package org.apache.hadoop.explorer.replicator.hms.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Соединение пользователя идет по delegation token метастора.
 *
 * <p>Открыть SASL-соединение прямо под пользователем нельзя: Kerberos требует его собственный
 * билет, которого у агента нет. Агент берет у метастора токен для пользователя и открывает
 * соединение с ним — тогда метастор видит пользователя, а не техучетку.
 */
@DisplayName("HMS: соединение пользователя открывается по delegation token")
class DelegationTokenConnectionTest {

    private static String tokenFor(String username) throws java.io.IOException {
        Token<?> token = new Token<>(
                username.getBytes(StandardCharsets.UTF_8),
                "secret".getBytes(StandardCharsets.UTF_8),
                new Text("HIVE_DELEGATION_TOKEN"),
                new Text("metastore"));
        return token.encodeToUrlString();
    }

    @Test
    @DisplayName("Токен кладется в контекст пользователя и подписывается для клиента метастора")
    void connectionCarriesTheUsersToken() throws Exception {
        List<String> openedAs = new CopyOnWriteArrayList<>();
        List<String> signatures = new CopyOnWriteArrayList<>();
        List<Integer> tokenCounts = new CopyOnWriteArrayList<>();

        MetaStoreConnectionFactory factory = new MetaStoreConnectionFactory(
                "dc1", new Configuration(), null,
                conf -> {
                    UserGroupInformation current = UserGroupInformation.getCurrentUser();
                    openedAs.add(current.getShortUserName());
                    signatures.add(conf.get("hive.metastore.token.signature"));
                    tokenCounts.add(current.getTokens().size());
                    return mock(IMetaStoreClient.class);
                },
                DelegationTokenConnectionTest::tokenFor);

        factory.create("analyst@COMPANY.LOCAL");

        assertEquals(List.of("analyst"), openedAs, "соединение открывается в контексте пользователя");
        assertEquals(MetaStoreConnectionFactory.TOKEN_SIGNATURE, signatures.get(0),
                "клиент метастора ищет токен по этой подписи");
        assertEquals(1, tokenCounts.get(0), "токен пользователя должен лежать в его контексте");
    }

    @Test
    @DisplayName("Токен переиспользуется, а не запрашивается на каждое соединение")
    void tokenIsRequestedOncePerUser() throws Exception {
        AtomicInteger issued = new AtomicInteger();
        MetaStoreConnectionFactory factory = new MetaStoreConnectionFactory(
                "dc1", new Configuration(), null,
                conf -> mock(IMetaStoreClient.class),
                username -> {
                    issued.incrementAndGet();
                    return tokenFor(username);
                });

        factory.create("analyst@COMPANY.LOCAL");
        factory.create("analyst@COMPANY.LOCAL");
        factory.create("etl@COMPANY.LOCAL");

        assertEquals(2, issued.get(), "по одному токену на пользователя, а не на соединение");
    }

    @Test
    @DisplayName("Отказ при открытии соединения сбрасывает запомненный токен")
    void failedConnectionForgetsTheToken() {
        AtomicInteger issued = new AtomicInteger();
        MetaStoreConnectionFactory factory = new MetaStoreConnectionFactory(
                "dc1", new Configuration(), null,
                conf -> {
                    throw new IllegalStateException("токен отозван");
                },
                username -> {
                    issued.incrementAndGet();
                    return tokenFor(username);
                });

        for (int attempt = 0; attempt < 2; attempt++) {
            Exception failure = null;
            try {
                factory.create("analyst@COMPANY.LOCAL");
            } catch (Exception e) {
                failure = e;
            }
            assertNotNull(failure, "открытие соединения должно было провалиться");
        }

        assertTrue(issued.get() == 2,
                "после отказа токен берется заново, а не используется протухший: запрошено " + issued.get());
    }
}
