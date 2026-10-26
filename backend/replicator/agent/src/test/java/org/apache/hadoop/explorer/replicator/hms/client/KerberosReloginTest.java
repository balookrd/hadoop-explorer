package org.apache.hadoop.explorer.replicator.hms.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.security.PrivilegedExceptionAction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Новое SASL-соединение открывается только после проверки TGT агента.
 *
 * <p>Bootstrap схемы на сотни тысяч партиций длится дольше срока жизни билета. Без перелогина
 * пул, вытеснив простаивающее соединение, уже не сможет открыть новое.
 */
@DisplayName("HMS: TGT агента обновляется перед открытием соединения")
class KerberosReloginTest {

    @SuppressWarnings("unchecked")
    private static UserGroupInformation keytabUgi(boolean fromKeytab) throws Exception {
        UserGroupInformation ugi = mock(UserGroupInformation.class);
        when(ugi.isFromKeytab()).thenReturn(fromKeytab);
        when(ugi.getShortUserName()).thenReturn("replicator");
        when(ugi.doAs(any(PrivilegedExceptionAction.class)))
                .thenAnswer(inv -> ((PrivilegedExceptionAction<Object>) inv.getArgument(0)).run());
        return ugi;
    }

    @Test
    @DisplayName("Соединение агента: перелогин перед открытием")
    @SuppressWarnings("unchecked")
    void agentConnectionReloginsFirst() throws Exception {
        UserGroupInformation ugi = keytabUgi(true);
        MetaStoreConnectionFactory factory = new MetaStoreConnectionFactory(
                "dc1", new Configuration(), ugi, conf -> mock(IMetaStoreClient.class));

        factory.create(MetaStoreConnectionFactory.AGENT_KEY);

        InOrder order = inOrder(ugi);
        order.verify(ugi).checkTGTAndReloginFromKeytab();
        order.verify(ugi).doAs(any(PrivilegedExceptionAction.class));
    }

    @Test
    @DisplayName("Выдача delegation token: перелогин перед обращением к метастору")
    void tokenIssueReloginsFirst() throws Exception {
        UserGroupInformation ugi = keytabUgi(true);
        IMetaStoreClient client = mock(IMetaStoreClient.class);
        when(client.getDelegationToken("analyst", "replicator")).thenReturn("token");
        MetaStoreConnectionFactory factory = new MetaStoreConnectionFactory(
                "dc1", new Configuration(), ugi, conf -> client);

        try {
            factory.create("analyst@COMPANY.LOCAL");
        } catch (Exception ignored) {
            // строка "token" не декодируется в Token — важен лишь порядок вызовов до этого
        }

        verify(ugi).checkTGTAndReloginFromKeytab();
        verify(client).getDelegationToken("analyst", "replicator");
    }

    @Test
    @DisplayName("UGI не из keytab (билет из кеша): перелогин не вызывается")
    void ticketCacheUgiIsLeftAlone() throws Exception {
        UserGroupInformation ugi = keytabUgi(false);
        MetaStoreConnectionFactory factory = new MetaStoreConnectionFactory(
                "dc1", new Configuration(), ugi, conf -> mock(IMetaStoreClient.class));

        factory.create(MetaStoreConnectionFactory.AGENT_KEY);

        verify(ugi, never()).checkTGTAndReloginFromKeytab();
    }
}
