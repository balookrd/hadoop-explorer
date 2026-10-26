package org.apache.hadoop.explorer.replicator.hms.client;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.explorer.replicator.security.KerberosUgiProvider;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Фабрика соединений с метастором, ключованная по принципалу.
 *
 * <p>SASL-аутентификация и {@code set_ugi} происходят при установке соединения, а не на каждый
 * вызов, поэтому общий пул под техучеткой агента делал имперсонацию видимостью: метастор видел
 * агента, что бы ни было написано в логе.
 *
 * <p>Открыть соединение прямо под пользователем нельзя: Kerberos требует его собственный билет,
 * которого у агента нет, и SASL падает с «Failed to find any Kerberos tgt» — это проверено на
 * стенде. Штатный путь — delegation token: агент по своему keytab просит у метастора токен для
 * пользователя, и соединение пользователя идет по DIGEST-MD5 с этим токеном. Метастор записывает
 * владельцем созданных объектов самого пользователя.
 */
class MetaStoreConnectionFactory extends BaseKeyedPooledObjectFactory<String, IMetaStoreClient> {

    private static final Logger log = LoggerFactory.getLogger(MetaStoreConnectionFactory.class);

    /** Ключ соединений самого агента: переноса от чьего-либо имени нет. */
    static final String AGENT_KEY = "";

    /** Под этой подписью токен кладется в UGI и ищется клиентом метастора. */
    static final String TOKEN_SIGNATURE = "replicator-hms-token";

    /** Как долго переиспользуется выданный токен: метастор выдает его на срок куда больший. */
    private static final long TOKEN_TTL_NANOS = 60L * 60 * 1_000_000_000L;

    /** Открытие соединения: подменяется в тестах, в бою это {@code new HiveMetaStoreClient(conf)}. */
    interface Opener {
        IMetaStoreClient open(Configuration conf) throws Exception;
    }

    /** Выдача delegation token метастора для пользователя. */
    interface TokenIssuer {
        String issue(String username) throws Exception;
    }

    private record CachedToken(String value, long issuedAtNanos) {
    }

    private final String clusterId;
    private final Configuration conf;
    private final UserGroupInformation agentUgi;
    private final Opener opener;
    private final TokenIssuer tokenIssuer;
    private final Map<String, CachedToken> tokenCache = new ConcurrentHashMap<>();

    MetaStoreConnectionFactory(String clusterId, Configuration conf, UserGroupInformation agentUgi, Opener opener) {
        this(clusterId, conf, agentUgi, opener, defaultIssuer(conf, agentUgi, opener));
    }

    MetaStoreConnectionFactory(String clusterId, Configuration conf, UserGroupInformation agentUgi,
                               Opener opener, TokenIssuer tokenIssuer) {
        this.clusterId = clusterId;
        this.conf = conf;
        this.agentUgi = agentUgi;
        this.opener = opener;
        this.tokenIssuer = tokenIssuer;
    }

    /**
     * Токен запрашивается по отдельному соединению агента, а не по взятому из пула: пул ключован
     * по принципалу, и обращаться к нему из его же фабрики — верный способ получить взаимную
     * блокировку при исчерпании соединений.
     */
    private static TokenIssuer defaultIssuer(Configuration conf, UserGroupInformation agentUgi, Opener opener) {
        if (agentUgi == null) {
            return null;
        }
        return username -> agentUgi.doAs((PrivilegedExceptionAction<String>) () -> {
            try (IMetaStoreClient client = opener.open(conf)) {
                return client.getDelegationToken(username, agentUgi.getShortUserName());
            }
        });
    }

    @Override
    public IMetaStoreClient create(String principalKey) throws Exception {
        if (principalKey == null || principalKey.isBlank()) {
            return openAsAgent();
        }
        KerberosUgiProvider.validatePrincipalSafety(principalKey);
        String username = KerberosUgiProvider.extractUsername(principalKey);
        if (tokenIssuer == null) {
            // Незащищенный контур: Kerberos нет, имя пользователя уходит в set_ugi как есть
            log.info("[ThriftHmsClient:{}] Соединение с метастором от имени '{}' без Kerberos", clusterId, username);
            return UserGroupInformation.createRemoteUser(username)
                    .doAs((PrivilegedExceptionAction<IMetaStoreClient>) () -> opener.open(conf));
        }
        return openWithDelegationToken(username);
    }

    private IMetaStoreClient openAsAgent() throws Exception {
        if (agentUgi == null) {
            return opener.open(conf);
        }
        return agentUgi.doAs((PrivilegedExceptionAction<IMetaStoreClient>) () -> opener.open(conf));
    }

    private IMetaStoreClient openWithDelegationToken(String username) throws Exception {
        String tokenString = tokenFor(username);
        UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(username);
        Token<?> token = new Token<>();
        token.decodeFromUrlString(tokenString);
        token.setService(new Text(TOKEN_SIGNATURE));
        userUgi.addToken(token);

        Configuration userConf = new Configuration(conf);
        userConf.set("hive.metastore.token.signature", TOKEN_SIGNATURE);

        log.info("[ThriftHmsClient:{}] Соединение с метастором от имени '{}' по delegation token", clusterId, username);
        try {
            return userUgi.doAs((PrivilegedExceptionAction<IMetaStoreClient>) () -> opener.open(userConf));
        } catch (Exception e) {
            // Протухший или отозванный токен: следующая попытка возьмет новый
            tokenCache.remove(username);
            throw e;
        }
    }

    private String tokenFor(String username) throws Exception {
        CachedToken cached = tokenCache.get(username);
        if (cached != null && System.nanoTime() - cached.issuedAtNanos() < TOKEN_TTL_NANOS) {
            return cached.value();
        }
        String issued = tokenIssuer.issue(username);
        if (issued == null || issued.isBlank()) {
            throw new IllegalStateException("Метастор не выдал delegation token для пользователя " + username);
        }
        tokenCache.put(username, new CachedToken(issued, System.nanoTime()));
        return issued;
    }

    @Override
    public PooledObject<IMetaStoreClient> wrap(IMetaStoreClient client) {
        return new DefaultPooledObject<>(client);
    }

    @Override
    public void destroyObject(String principalKey, PooledObject<IMetaStoreClient> p) {
        try {
            p.getObject().close();
        } catch (Exception ignored) {
            // соединение уже оборвано — закрывать нечего
        }
    }

    @Override
    public boolean validateObject(String principalKey, PooledObject<IMetaStoreClient> p) {
        return p != null && p.getObject() != null;
    }
}
