package org.apache.hadoop.explorer.replicator.hms.client;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Соединение с метастором открывается от имени того, чьи данные переносятся.
 *
 * <p>SASL-аутентификация и {@code set_ugi} происходят в момент установки соединения, а не на
 * каждый вызов. Пока пул был общим и открывался под техучеткой агента, {@code doAs} вокруг
 * вызова менял Subject только внутри JVM агента: метастор и Ranger видели агента, а запись
 * {@code doAs=user@REALM} в логе отражала одно лишь намерение.
 */
@DisplayName("HMS: соединение открывается под пользователем, от чьего имени идет перенос")
class ImpersonatedConnectionTest {

    /** Кто был текущим пользователем в момент открытия каждого соединения. */
    private final List<String> openedAs = new CopyOnWriteArrayList<>();

    private ThriftHmsClient client;

    @BeforeEach
    void setUp() {
        client = new ThriftHmsClient("dc1", "thrift://localhost:9083", conf -> {
            openedAs.add(UserGroupInformation.getCurrentUser().getShortUserName());
            IMetaStoreClient metaStore = mock(IMetaStoreClient.class);
            when(metaStore.getAllDatabases()).thenReturn(List.of("default"));
            return metaStore;
        }, 4);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    @DisplayName("Разные пользователи получают разные соединения, открытые от их имени")
    void eachPrincipalGetsItsOwnConnection() throws Exception {
        client.executeAs("alice@COMPANY.LOCAL", () -> client.getAllDatabases());
        client.executeAs("bob@COMPANY.LOCAL", () -> client.getAllDatabases());

        assertEquals(List.of("alice", "bob"), openedAs,
                "каждое соединение открывается под своим пользователем, а не под агентом");
    }

    @Test
    @DisplayName("Повторный перенос того же пользователя переиспользует его соединение")
    void samePrincipalReusesItsConnection() throws Exception {
        client.executeAs("alice@COMPANY.LOCAL", () -> client.getAllDatabases());
        client.executeAs("alice@COMPANY.LOCAL", () -> client.getAllDatabases());
        client.executeAs("alice@COMPANY.LOCAL", () -> client.getAllDatabases());

        assertEquals(List.of("alice"), openedAs,
                "соединение пользователя берется из пула, а не открывается заново");
    }

    @Test
    @DisplayName("Без пользователя соединение идет под техучеткой агента")
    void withoutPrincipalTheAgentOwnsTheConnection() throws Exception {
        client.getAllDatabases();

        assertEquals(1, openedAs.size(), "должно открыться одно соединение");
        assertTrue(openedAs.get(0).equals(UserGroupInformation.getCurrentUser().getShortUserName()),
                "без имперсонации соединение открывается под самим процессом агента");
    }
}
