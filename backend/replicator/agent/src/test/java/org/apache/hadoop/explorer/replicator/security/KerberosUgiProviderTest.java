package org.apache.hadoop.explorer.replicator.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Kerberos UGI Provider & Impersonation Security Tests")
class KerberosUgiProviderTest {

    @Test
    @DisplayName("extractUsername: извлечение чистого имени из principal")
    void testExtractUsername() {
        assertEquals("hdfs", KerberosUgiProvider.extractUsername(null));
        assertEquals("hdfs", KerberosUgiProvider.extractUsername(""));
        assertEquals("hdfs", KerberosUgiProvider.extractUsername("   "));
        assertEquals("alice", KerberosUgiProvider.extractUsername("alice@REALM.CORP"));
        assertEquals("hdfs", KerberosUgiProvider.extractUsername("hdfs/nn1.corp.net@REALM.CORP"));
        assertEquals("bob", KerberosUgiProvider.extractUsername("bob"));
        assertEquals("etl_service", KerberosUgiProvider.extractUsername("etl_service@EXAMPLE.COM"));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "root",
            "root@REALM.LOCAL",
            "daemon/host@REALM",
            "bin",
            "nobody",
            "0"
    })
    @DisplayName("validatePrincipalSafety: блокировка системных учетных записей ОС")
    void testValidatePrincipalSafetyBlockedForbidden(String principal) {
        assertThrows(SecurityException.class, () -> {
            KerberosUgiProvider.validatePrincipalSafety(principal);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "user;rm -rf /",
            "alice\0injected",
            "../etc/passwd",
            "user\nadmin",
            "bad|pipe"
    })
    @DisplayName("validatePrincipalSafety: блокировка недопустимых символов и инъекций")
    void testValidatePrincipalSafetyBlockedInjections(String malicious) {
        assertThrows(SecurityException.class, () -> {
            KerberosUgiProvider.validatePrincipalSafety(malicious);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "alice",
            "alice@COMPANY.COM",
            "hdfs/nn-active.datacenter.lan@COMPANY.COM",
            "spark_user",
            "data-engineer.service@CORP.NET"
    })
    @DisplayName("validatePrincipalSafety: разрешенные легитимные Kerberos принципалы")
    void testValidatePrincipalSafetyValid(String validPrincipal) {
        assertDoesNotThrow(() -> KerberosUgiProvider.validatePrincipalSafety(validPrincipal));
    }

    @Test
    @DisplayName("getEffectiveUgi: получение UGI без Kerberos возвращает текущего пользователя")
    void testGetEffectiveUgiWithoutKerberos() throws IOException {
        UserGroupInformation testUser = UserGroupInformation.createUserForTesting("test-service", new String[]{"hadoop"});
        UserGroupInformation.setLoginUser(testUser);

        Configuration conf = new Configuration();
        KerberosUgiProvider provider = new KerberosUgiProvider(conf, "hdfs-replicator@REALM", null);

        UserGroupInformation ugi = provider.getEffectiveUgi("alice@COMPANY.COM", false);
        assertNotNull(ugi);
        assertFalse(provider.isKerberosLoggedIn());
        assertEquals("hdfs-replicator@REALM", provider.getServicePrincipal());
    }
}
