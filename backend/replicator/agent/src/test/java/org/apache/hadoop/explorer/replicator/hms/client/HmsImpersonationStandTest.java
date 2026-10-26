package org.apache.hadoop.explorer.replicator.hms.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.security.PrivilegedExceptionAction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Имперсонация на живом метасторе демо-стенда (Apache Hive 4.0.0 под Kerberos).
 *
 * <p>Выключен по умолчанию: нужен поднятый стенд. Запуск:
 * <pre>
 * docker compose -p hadoop-explorer-shared-hms \
 *   -f demo/platform/docker-compose.hms.yml -f demo/platform/docker-compose.hdfs.yml \
 *   up -d kdc hdfs-cluster-1 sql-hive-metastore
 * docker cp kdc:/shared/conf/krb5.conf /tmp/krb5.conf   # kdc заменить на 127.0.0.1
 * docker cp kdc:/shared/keytabs/hdfs-cluster-1.keytab /tmp/
 * KRB5_PRINCIPAL=hdfs/hdfs-cluster-1@COMPANY.LOCAL mvn test -f backend/replicator/pom.xml \
 *   -pl agent -am -Dtest=HmsImpersonationStandTest -Dhms.stand=true \
 *   -Dhms.keytab=/tmp/hdfs-cluster-1.keytab -Djava.security.krb5.conf=/tmp/krb5.conf
 * </pre>
 *
 * <p>Проверяется то единственное, что имеет значение: кого метастор записал владельцем базы,
 * созданной от имени пользователя. До перехода на delegation token там стоял агент.
 */
@EnabledIfSystemProperty(named = "hms.stand", matches = "true")
@DisplayName("Стенд: метастор видит пользователя, а не агента")
class HmsImpersonationStandTest {

    private static final String DB = "impersonation_probe";
    private static final String USER = "analyst";
    private static final String HMS_URI = "thrift://localhost:9083";
    private static final String HMS_PRINCIPAL = "hive/hive-metastore@COMPANY.LOCAL";

    @Test
    @DisplayName("Владельцем базы, созданной от имени analyst, метастор записывает analyst")
    void metastoreRecordsTheUserAsOwner() throws Exception {
        String keytab = System.getProperty("hms.keytab");
        assertTrue(keytab != null && !keytab.isBlank(), "нужен -Dhms.keytab");

        dropProbeDatabase(keytab);

        try (ThriftHmsClient client = new ThriftHmsClient("dc1", HMS_URI, HMS_PRINCIPAL, keytab, 4)) {
            client.executeAs(USER + "@COMPANY.LOCAL", () -> {
                client.createDatabase(DB, null);
                return null;
            });
        }

        assertEquals(USER, ownerOfProbeDatabase(keytab),
                "метастор обязан видеть пользователя, от чьего имени идет перенос");
    }

    /** Чтение результата сторонним соединением: проверяем не себя, а то, что записал метастор. */
    private String ownerOfProbeDatabase(String keytab) throws Exception {
        return asAgent(keytab, client -> {
            Database database = client.getDatabase(DB);
            return database.getOwnerName();
        });
    }

    private void dropProbeDatabase(String keytab) throws Exception {
        asAgent(keytab, client -> {
            try {
                client.dropDatabase(DB, true, true, true);
            } catch (Exception ignored) {
                // базы еще нет — это и требуется
            }
            return null;
        });
    }

    private interface MetaStoreAction<T> {
        T run(IMetaStoreClient client) throws Exception;
    }

    private <T> T asAgent(String keytab, MetaStoreAction<T> action) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hive.metastore.uris", HMS_URI);
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hive.metastore.sasl.enabled", "true");
        conf.set("hive.metastore.kerberos.principal", HMS_PRINCIPAL);
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation agent = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                System.getenv("KRB5_PRINCIPAL"), keytab);
        return agent.doAs((PrivilegedExceptionAction<T>) () -> {
            try (IMetaStoreClient client = new HiveMetaStoreClient(conf)) {
                return action.run(client);
            }
        });
    }
}
