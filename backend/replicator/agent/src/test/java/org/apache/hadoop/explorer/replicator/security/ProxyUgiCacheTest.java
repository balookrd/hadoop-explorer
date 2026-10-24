package org.apache.hadoop.explorer.replicator.security;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Переиспользование proxy-UGI при doAs-имперсонации.
 *
 * <p>Ключевая гарантия (OOM): {@code FileSystem.Cache} ключуется по UGI, а {@code UGI.equals}
 * сравнивает {@code Subject} по ссылке. Новый proxy-UGI на каждую файловую операцию означает
 * новый закэшированный {@code DistributedFileSystem} с собственными сокетами и
 * lease-renewer-потоком: синхронизация каталога из сотен тысяч файлов исчерпывала
 * файловые дескрипторы и валила агент по памяти.
 */
@DisplayName("Кэш proxy-UGI для doAs")
class ProxyUgiCacheTest {

    @Test
    @DisplayName("Повторная имперсонация того же пользователя переиспользует UGI")
    void testSameUserReusesProxyUgi() {
        KerberosUgiProvider provider = new KerberosUgiProvider();
        UserGroupInformation base = UserGroupInformation.createRemoteUser("replicator-svc");

        UserGroupInformation first = provider.proxyUserFor("alice", base);
        UserGroupInformation second = provider.proxyUserFor("alice", base);

        assertSame(first, second,
                "Новый proxy-UGI на каждую операцию порождает новый DFSClient в FileSystem.CACHE");
        assertEquals("alice", first.getShortUserName());
    }

    @Test
    @DisplayName("Разные пользователи получают разные UGI")
    void testDifferentUsersGetDistinctUgi() {
        KerberosUgiProvider provider = new KerberosUgiProvider();
        UserGroupInformation base = UserGroupInformation.createRemoteUser("replicator-svc");

        UserGroupInformation alice = provider.proxyUserFor("alice", base);
        UserGroupInformation bob = provider.proxyUserFor("bob", base);

        assertNotSame(alice, bob, "Имперсонация разных пользователей не может делить один UGI");
        assertEquals("bob", bob.getShortUserName());
    }

    @Test
    @DisplayName("Смена системной техучетки инвалидирует кэш")
    void testBaseUgiChangeInvalidatesCache() {
        KerberosUgiProvider provider = new KerberosUgiProvider();
        UserGroupInformation base = UserGroupInformation.createRemoteUser("replicator-svc");
        UserGroupInformation rotatedBase = UserGroupInformation.createRemoteUser("replicator-svc-2");

        UserGroupInformation underFirst = provider.proxyUserFor("alice", base);
        UserGroupInformation underRotated = provider.proxyUserFor("alice", rotatedBase);

        assertNotSame(underFirst, underRotated,
                "После перелогина техучетки proxy-UGI обязан быть создан заново");
    }
}
