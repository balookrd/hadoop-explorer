package org.apache.hadoop.explorer.replicator.hms.client;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.explorer.replicator.hms.model.HmsFieldSchemaDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsNotificationEventDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.TException;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.*;

/**
 * Высокопроизводительная потокобезопасная боевая реализация HmsClient через Thrift RPC протокол Hive Metastore.
 * Инкапсулирует пул соединений IMetaStoreClient (Apache Commons-Pool2), предотвращая гонки потоков,
 * поддерживает Kerberos SASL аутентификацию, doAs имперсонацию и перенос полной схемы колонок и партиций без хардкода.
 */
public class ThriftHmsClient implements HmsClient, Closeable {

    private static final Logger log = LoggerFactory.getLogger(ThriftHmsClient.class);

    private final String clusterId;
    private final String thriftUris;
    private final String kerberosPrincipal;
    private final String keytabPath;
    private final Configuration conf;
    private final UserGroupInformation ugi;
    private final GenericObjectPool<IMetaStoreClient> clientPool;
    private final ThreadLocal<String> currentExecutionPrincipal = new ThreadLocal<>();

    public ThriftHmsClient(String clusterId, String thriftUris, String kerberosPrincipal, String keytabPath) {
        this(clusterId, thriftUris, kerberosPrincipal, keytabPath, 16);
    }

    public ThriftHmsClient(String clusterId, String thriftUris, String kerberosPrincipal, String keytabPath, int maxPoolSize) {
        this.clusterId = clusterId;
        this.thriftUris = thriftUris;
        this.kerberosPrincipal = kerberosPrincipal;
        this.keytabPath = keytabPath;

        this.conf = new Configuration();
        if (thriftUris != null && !thriftUris.isBlank()) {
            this.conf.set("hive.metastore.uris", thriftUris);
        }

        // Kerberos / SASL конфигурация Hive Metastore и изоляция UGI
        UserGroupInformation resolvedUgi = null;
        if ((kerberosPrincipal != null && !kerberosPrincipal.isBlank()) || UserGroupInformation.isSecurityEnabled()) {
            this.conf.set("hadoop.security.authentication", "kerberos");
            this.conf.set("hive.metastore.sasl.enabled", "true");
            this.conf.set("hive.metastore.kerberos.principal", kerberosPrincipal != null ? kerberosPrincipal : "");
            if (keytabPath != null && !keytabPath.isBlank()) {
                this.conf.set("hive.metastore.kerberos.keytab.file", keytabPath);
            }
            try {
                UserGroupInformation.setConfiguration(conf);
                if (keytabPath != null && !keytabPath.isBlank()) {
                    String userPrinc = System.getenv("KRB5_PRINCIPAL");
                    if (userPrinc == null || userPrinc.isBlank()) {
                        userPrinc = kerberosPrincipal;
                    }
                    resolvedUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(userPrinc, keytabPath);
                } else if (UserGroupInformation.isSecurityEnabled()) {
                    resolvedUgi = UserGroupInformation.getLoginUser();
                }
            } catch (Exception e) {
                log.warn("[ThriftHmsClient:{}] Ошибка инициализации Kerberos UGI: {}", clusterId, e.getMessage());
            }
        }
        this.ugi = resolvedUgi;

        GenericObjectPoolConfig<IMetaStoreClient> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(Math.max(4, maxPoolSize));
        poolConfig.setMaxIdle(Math.max(2, maxPoolSize / 2));
        poolConfig.setMinIdle(1);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWait(Duration.ofSeconds(30));

        this.clientPool = new GenericObjectPool<>(new MetaStoreClientFactory(), poolConfig);
        log.info("[ThriftHmsClient:{}] Инициализирован пул Thrift клиентов к '{}' (maxTotal={}, kerberos={})",
                clusterId, thriftUris, poolConfig.getMaxTotal(), kerberosPrincipal != null);
    }

    ThriftHmsClient(String clusterId, String thriftUris, GenericObjectPool<IMetaStoreClient> clientPool) {
        this.clusterId = clusterId;
        this.thriftUris = thriftUris != null ? thriftUris : "thrift://localhost:9083";
        this.kerberosPrincipal = null;
        this.keytabPath = null;
        this.conf = new org.apache.hadoop.conf.Configuration();
        this.clientPool = clientPool;
        this.ugi = null;
    }

    public GenericObjectPool<IMetaStoreClient> getClientPool() {
        return clientPool;
    }

    private class MetaStoreClientFactory extends BasePooledObjectFactory<IMetaStoreClient> {
        @Override
        public IMetaStoreClient create() throws Exception {
            if (ugi != null) {
                log.info("[ThriftHmsClient:{}] Создание нового Thrift клиента с Kerberos UGI: {}", clusterId, ugi.getUserName());
                return ugi.doAs((PrivilegedExceptionAction<IMetaStoreClient>) () -> new HiveMetaStoreClient(conf));
            } else {
                log.info("[ThriftHmsClient:{}] Создание нового Thrift клиента без Kerberos", clusterId);
                return new HiveMetaStoreClient(conf);
            }
        }

        @Override
        public PooledObject<IMetaStoreClient> wrap(IMetaStoreClient client) {
            return new DefaultPooledObject<>(client);
        }

        @Override
        public void destroyObject(PooledObject<IMetaStoreClient> p) {
            try {
                p.getObject().close();
            } catch (Exception ignored) {}
        }

        @Override
        public boolean validateObject(PooledObject<IMetaStoreClient> p) {
            try {
                if (p == null || p.getObject() == null) {
                    return false;
                }
                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }

    @Override
    /**
     * Выполнение операций от имени пользователя.
     *
     * <p><b>Ограничение, о котором нужно знать.</b> Соединения берутся из общего пула, открытого
     * под техучеткой агента, а SASL-аутентификация и {@code set_ugi} происходят в момент
     * установки соединения. {@code doAs} вокруг вызова меняет Subject только на стороне JVM
     * агента, поэтому метастор и Ranger видят принципала агента, а не пользователя: запись в
     * логе {@code doAs=user@REALM} отражает намерение, а не то, что увидел HMS.
     *
     * <p>Полноценная имперсонация требует пула, ключованного по принципалу, с открытием
     * соединения внутри {@code doAs} и {@code hive.metastore.execute.setugi=true}. Это отдельная
     * работа: у пула меняются жизненный цикл, вытеснение и учет соединений.
     */
    public <T> T executeAs(String executionPrincipal, java.util.concurrent.Callable<T> action) throws Exception {
        if (executionPrincipal == null || executionPrincipal.isBlank()) {
            return action.call();
        }
        org.apache.hadoop.explorer.replicator.security.KerberosUgiProvider.validatePrincipalSafety(executionPrincipal);
        currentExecutionPrincipal.set(executionPrincipal);
        try {
            return action.call();
        } finally {
            currentExecutionPrincipal.remove();
        }
    }

    private UserGroupInformation resolveEffectiveUgi() {
        String principal = currentExecutionPrincipal.get();
        if (principal == null || principal.isBlank()) {
            return this.ugi;
        }
        org.apache.hadoop.explorer.replicator.security.KerberosUgiProvider.validatePrincipalSafety(principal);
        String username = org.apache.hadoop.explorer.replicator.security.KerberosUgiProvider.extractUsername(principal);
        if (this.ugi != null) {
            return UserGroupInformation.createProxyUser(username, this.ugi);
        } else {
            return UserGroupInformation.createRemoteUser(username);
        }
    }

    private <T> T doExecute(IMetaStoreClient client, HmsOperation<T> operation) throws Exception {
        UserGroupInformation effectiveUgi = resolveEffectiveUgi();
        if (effectiveUgi != null) {
            try {
                return effectiveUgi.doAs((PrivilegedExceptionAction<T>) () -> operation.execute(client));
            } catch (java.lang.reflect.UndeclaredThrowableException ute) {
                Throwable cause = ute.getCause();
                if (cause instanceof Exception e) throw e;
                throw ute;
            }
        } else {
            return operation.execute(client);
        }
    }

    private <T> T executeWithRetry(HmsOperation<T> operation) {
        IMetaStoreClient client = null;
        try {
            client = clientPool.borrowObject();
            return doExecute(client, operation);
        } catch (TException e) {
            log.warn("[ThriftHmsClient:{}] Ошибка Thrift/MetaException ({}): {}, попытка повтора с новым соединением...",
                    clusterId, e.getClass().getSimpleName(), e.getMessage());
            if (client != null) {
                try {
                    clientPool.invalidateObject(client);
                } catch (Exception ignored) {}
                client = null;
            }
            try {
                client = clientPool.borrowObject();
                return doExecute(client, operation);
            } catch (Exception ex) {
                if (client != null) {
                    try {
                        clientPool.invalidateObject(client);
                    } catch (Exception ignored) {}
                    client = null;
                }
                throw new RuntimeException("Ошибка выполнения операции Hive Metastore: " + ex.getMessage(), ex);
            }
        } catch (Exception e) {
            if (client != null && isConnectionException(e)) {
                try {
                    clientPool.invalidateObject(client);
                } catch (Exception ignored) {}
                client = null;
            }
            throw new RuntimeException("Ошибка выполнения операции Hive Metastore: " + e.getMessage(), e);
        } finally {
            if (client != null) {
                clientPool.returnObject(client);
            }
        }
    }

    private boolean isConnectionException(Throwable t) {
        if (t == null) return false;
        if (t instanceof java.net.SocketException
                || t instanceof java.io.EOFException
                || t instanceof org.apache.thrift.transport.TTransportException) {
            return true;
        }
        return isConnectionException(t.getCause());
    }

    @FunctionalInterface
    private interface HmsOperation<T> {
        T execute(IMetaStoreClient client) throws Exception;
    }

    @Override
    public long getCurrentNotificationEventId() {
        try {
            return executeWithRetry(c -> {
                CurrentNotificationEventId eventId = c.getCurrentNotificationEventId();
                return eventId != null ? eventId.getEventId() : 0L;
            });
        } catch (Exception e) {
            log.warn("[ThriftHmsClient:{}] Не удалось прочитать CurrentNotificationEventId ({}), fallback на 0L",
                    clusterId, e.getMessage());
            return 0L;
        }
    }

    @Override
    public List<HmsNotificationEventDto> getNextNotifications(long lastEventId, int maxEvents) {
        try {
            return executeWithRetry(c -> {
                NotificationEventResponse resp = c.getNextNotification(lastEventId, maxEvents, null);
                if (resp == null || resp.getEvents() == null) {
                    return Collections.emptyList();
                }
                List<HmsNotificationEventDto> list = new ArrayList<>(resp.getEvents().size());
                for (NotificationEvent event : resp.getEvents()) {
                    list.add(new HmsNotificationEventDto(
                            event.getEventId(),
                            event.getEventTime(),
                            event.getEventType(),
                            event.getDbName(),
                            event.getTableName(),
                            event.getMessage(),
                            event.getMessageFormat()
                    ));
                }
                return list;
            });
        } catch (Exception e) {
            log.warn("[ThriftHmsClient:{}] Не удалось прочитать события из NotificationLog ({}), возврат пустого списка",
                    clusterId, e.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public List<String> getAllDatabases() {
        return executeWithRetry(IMetaStoreClient::getAllDatabases);
    }

    @Override
    public Optional<String> getDatabaseLocation(String dbName) {
        try {
            return executeWithRetry(c -> {
                try {
                    Database db = c.getDatabase(dbName);
                    return Optional.ofNullable(db != null ? db.getLocationUri() : null);
                } catch (NoSuchObjectException e) {
                    return Optional.empty();
                }
            });
        } catch (Exception e) {
            log.warn("[ThriftHmsClient:{}] Не удалось получить location для БД '{}': {}", clusterId, dbName, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public List<String> getAllTables(String dbName) {
        return executeWithRetry(c -> c.getAllTables(dbName));
    }

    @Override
    public Optional<HmsTableDto> getTable(String dbName, String tableName) {
        return executeWithRetry(c -> {
            try {
                Table table = c.getTable(dbName, tableName);
                return Optional.ofNullable(toDto(table));
            } catch (NoSuchObjectException e) {
                return Optional.empty();
            }
        });
    }

    @Override
    public List<HmsPartitionDto> getPartitions(String dbName, String tableName) {
        return executeWithRetry(c -> {
            try {
                List<Partition> partitions = c.listPartitions(dbName, tableName, (short) -1);
                if (partitions == null) return Collections.emptyList();
                List<HmsPartitionDto> list = new ArrayList<>(partitions.size());
                for (Partition p : partitions) {
                    list.add(toDto(p));
                }
                return list;
            } catch (NoSuchObjectException e) {
                return Collections.emptyList();
            }
        });
    }

    @Override
    public Optional<HmsPartitionDto> getPartition(String dbName, String tableName, List<String> partVals) {
        if (partVals == null || partVals.isEmpty()) {
            return Optional.empty();
        }
        return executeWithRetry(c -> {
            try {
                Partition partition = c.getPartition(dbName, tableName, partVals);
                return Optional.ofNullable(toDto(partition));
            } catch (NoSuchObjectException e) {
                return Optional.empty();
            }
        });
    }

    @Override
    public void createDatabase(String dbName, String locationUri) {
        executeWithRetry(c -> {
            try {
                Database db = new Database();
                db.setName(dbName);
                if (locationUri != null && !locationUri.isBlank()) {
                    db.setLocationUri(locationUri);
                }
                c.createDatabase(db);
                log.info("[ThriftHmsClient:{}] База данных '{}' успешно создана в Hive Metastore", clusterId, dbName);
            } catch (AlreadyExistsException e) {
                log.debug("[ThriftHmsClient:{}] База данных '{}' уже существует в Hive Metastore", clusterId, dbName);
            }
            return null;
        });
    }

    @Override
    public void createTable(HmsTableDto tableDto) {
        executeWithRetry(c -> {
            try {
                Table table = toThrift(tableDto);
                c.createTable(table);
                log.info("[ThriftHmsClient:{}] Таблица '{}.{}' (колонок: {}, партиций: {}) успешно создана в Hive Metastore",
                        clusterId, tableDto.dbName(), tableDto.tableName(), tableDto.columns().size(), tableDto.partitionKeys().size());
            } catch (AlreadyExistsException e) {
                log.warn("[ThriftHmsClient:{}] Таблица '{}.{}' уже существует, выполняется alterTable",
                        clusterId, tableDto.dbName(), tableDto.tableName());
                alterTable(tableDto);
            }
            return null;
        });
    }

    @Override
    public void alterTable(HmsTableDto tableDto) {
        executeWithRetry(c -> {
            Table table = toThrift(tableDto);
            c.alter_table(tableDto.dbName(), tableDto.tableName(), table);
            log.info("[ThriftHmsClient:{}] Схема таблицы '{}.{}' успешно обновлена в Hive Metastore",
                    clusterId, tableDto.dbName(), tableDto.tableName());
            return null;
        });
    }

    /**
     * Идемпотентное применение батча партиций (upsert).
     *
     * <p>Используется {@code add_partitions(parts, ifNotExists=true, needResults=true)}: при коллизии
     * хотя бы одной партиции HMS не отбрасывает весь батч, а возвращает фактически добавленные.
     * Партиции, которые уже существовали, обновляются через {@code alter_partitions} — иначе
     * события {@code ALTER_PARTITION} (смена location или параметров) молча терялись бы, а ретрай
     * частично примененного батча никогда не доносил бы остаток.
     */
    @Override
    public void addPartitions(String dbName, String tableName, List<HmsPartitionDto> partitions) {
        if (partitions == null || partitions.isEmpty()) return;
        executeWithRetry(c -> {
            List<Partition> thriftPartitions = new ArrayList<>(partitions.size());
            for (HmsPartitionDto pDto : partitions) {
                thriftPartitions.add(toThrift(pDto));
            }

            List<Partition> added;
            try {
                added = c.add_partitions(thriftPartitions, true, true);
            } catch (AlreadyExistsException e) {
                // Метастор не поддержал ifNotExists: считаем, что не добавлено ничего, и обновляем все.
                log.info("[ThriftHmsClient:{}] Метастор отклонил батч как существующий в '{}.{}', переходим к обновлению",
                        clusterId, dbName, tableName);
                added = List.of();
            }

            int addedCount = added != null ? added.size() : 0;
            log.info("[ThriftHmsClient:{}] {} из {} партиций добавлено в '{}.{}' Hive Metastore",
                    clusterId, addedCount, thriftPartitions.size(), dbName, tableName);

            List<Partition> existing = partitionsNotAdded(thriftPartitions, added);
            if (!existing.isEmpty()) {
                c.alter_partitions(dbName, tableName, existing);
                log.info("[ThriftHmsClient:{}] {} уже существовавших партиций обновлено (upsert) в '{}.{}'",
                        clusterId, existing.size(), dbName, tableName);
            }
            return null;
        });
    }

    /** Дефолтный формат партиции для агентов, не передающих формат в gRPC-контракте. */
    private static final String DEFAULT_PARTITION_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat";
    private static final String DEFAULT_PARTITION_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
    private static final String DEFAULT_PARTITION_SERDE_LIB = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

    private static String firstNonBlank(String value, String fallback) {
        return (value != null && !value.isBlank()) ? value : fallback;
    }

    /**
     * Партиции запрошенного батча, которых нет в списке фактически добавленных HMS, —
     * то есть те, что уже существовали и требуют обновления метаданных.
     */
    private static List<Partition> partitionsNotAdded(List<Partition> requested, List<Partition> added) {
        if (added == null || added.isEmpty()) {
            return requested;
        }
        Set<List<String>> addedValues = new HashSet<>();
        for (Partition p : added) {
            addedValues.add(p.getValues() != null ? p.getValues() : List.of());
        }
        List<Partition> notAdded = new ArrayList<>();
        for (Partition p : requested) {
            List<String> values = p.getValues() != null ? p.getValues() : List.of();
            if (!addedValues.contains(values)) {
                notAdded.add(p);
            }
        }
        return notAdded;
    }

    @Override
    public void dropTable(String dbName, String tableName, boolean deleteData) {
        executeWithRetry(c -> {
            try {
                c.dropTable(dbName, tableName, deleteData, true);
                log.info("[ThriftHmsClient:{}] Таблица '{}.{}' удалена из Hive Metastore (deleteData={})",
                        clusterId, dbName, tableName, deleteData);
            } catch (NoSuchObjectException e) {
                log.debug("[ThriftHmsClient:{}] Таблица '{}.{}' уже отсутствует в Hive Metastore",
                        clusterId, dbName, tableName);
            }
            return null;
        });
    }

    @Override
    public void dropPartition(String dbName, String tableName, List<String> partVals, boolean deleteData) {
        executeWithRetry(c -> {
            try {
                c.dropPartition(dbName, tableName, partVals, deleteData);
                log.info("[ThriftHmsClient:{}] Партиция {} удалена из '{}.{}' Hive Metastore (deleteData={})",
                        clusterId, partVals, dbName, tableName, deleteData);
            } catch (NoSuchObjectException e) {
                log.debug("[ThriftHmsClient:{}] Партиция {} уже отсутствует в '{}.{}'",
                        clusterId, partVals, dbName, tableName);
            }
            return null;
        });
    }

    @Override
    public void close() throws IOException {
        if (clientPool != null) {
            clientPool.close();
        }
    }

    // =========================================================================
    // Конвертеры DTO <-> Thrift Entity с сохранением полной схемы и типов
    // =========================================================================

    public static HmsTableDto toDto(Table t) {
        if (t == null) return null;
        String location = (t.getSd() != null) ? t.getSd().getLocation() : null;
        String inputFormat = (t.getSd() != null) ? t.getSd().getInputFormat() : null;
        String outputFormat = (t.getSd() != null) ? t.getSd().getOutputFormat() : null;
        String serdeLib = (t.getSd() != null && t.getSd().getSerdeInfo() != null)
                ? t.getSd().getSerdeInfo().getSerializationLib() : null;
        Map<String, String> serdeParams = (t.getSd() != null && t.getSd().getSerdeInfo() != null && t.getSd().getSerdeInfo().getParameters() != null)
                ? new HashMap<>(t.getSd().getSerdeInfo().getParameters()) : Collections.emptyMap();

        List<String> partKeys = new ArrayList<>();
        List<HmsFieldSchemaDto> partKeyFields = new ArrayList<>();
        if (t.getPartitionKeys() != null) {
            for (FieldSchema fs : t.getPartitionKeys()) {
                partKeys.add(fs.getName());
                partKeyFields.add(new HmsFieldSchemaDto(fs.getName(), fs.getType(), fs.getComment()));
            }
        }

        List<HmsFieldSchemaDto> cols = new ArrayList<>();
        if (t.getSd() != null && t.getSd().getCols() != null) {
            for (FieldSchema fs : t.getSd().getCols()) {
                cols.add(new HmsFieldSchemaDto(fs.getName(), fs.getType(), fs.getComment()));
            }
        }

        return new HmsTableDto(
                t.getCatName() != null ? t.getCatName() : "hive",
                t.getDbName(),
                t.getTableName(),
                t.getTableType(),
                location,
                t.getParameters() != null ? new HashMap<>(t.getParameters()) : Collections.emptyMap(),
                partKeys,
                inputFormat,
                outputFormat,
                serdeLib,
                cols,
                partKeyFields,
                serdeParams
        );
    }

    public static Table toThrift(HmsTableDto dto) {
        Table t = new Table();
        t.setCatName(dto.catName() != null ? dto.catName() : "hive");
        t.setDbName(dto.dbName());
        t.setTableName(dto.tableName());
        t.setTableType(dto.tableType() != null ? dto.tableType() : "MANAGED_TABLE");

        if (dto.parameters() != null) {
            t.setParameters(new HashMap<>(dto.parameters()));
        }

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(dto.sdLocation());
        sd.setInputFormat(dto.inputFormat() != null ? dto.inputFormat() : "org.apache.hadoop.mapred.TextInputFormat");
        sd.setOutputFormat(dto.outputFormat() != null ? dto.outputFormat() : "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");

        SerDeInfo serde = new SerDeInfo();
        serde.setName(dto.tableName());
        serde.setSerializationLib(dto.serdeLib() != null ? dto.serdeLib() : "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        if (dto.serdeParameters() != null && !dto.serdeParameters().isEmpty()) {
            serde.setParameters(new HashMap<>(dto.serdeParameters()));
        }
        sd.setSerdeInfo(serde);

        List<FieldSchema> cols = new ArrayList<>();
        if (dto.columns() != null && !dto.columns().isEmpty()) {
            for (HmsFieldSchemaDto col : dto.columns()) {
                cols.add(new FieldSchema(col.name(), col.type(), col.comment()));
            }
        }
        sd.setCols(cols);
        t.setSd(sd);

        List<FieldSchema> pks = new ArrayList<>();
        if (dto.partitionKeyFields() != null && !dto.partitionKeyFields().isEmpty()) {
            for (HmsFieldSchemaDto pk : dto.partitionKeyFields()) {
                pks.add(new FieldSchema(pk.name(), pk.type(), pk.comment()));
            }
        } else if (dto.partitionKeys() != null && !dto.partitionKeys().isEmpty()) {
            for (String pk : dto.partitionKeys()) {
                pks.add(new FieldSchema(pk, "string", "partition key"));
            }
        }
        t.setPartitionKeys(pks);

        return t;
    }

    public static HmsPartitionDto toDto(Partition p) {
        if (p == null) return null;
        String location = (p.getSd() != null) ? p.getSd().getLocation() : null;
        List<HmsFieldSchemaDto> cols = new ArrayList<>();
        if (p.getSd() != null && p.getSd().getCols() != null) {
            for (FieldSchema fs : p.getSd().getCols()) {
                cols.add(new HmsFieldSchemaDto(fs.getName(), fs.getType(), fs.getComment()));
            }
        }
        Map<String, String> serdeParams = (p.getSd() != null && p.getSd().getSerdeInfo() != null && p.getSd().getSerdeInfo().getParameters() != null)
                ? new HashMap<>(p.getSd().getSerdeInfo().getParameters()) : Collections.emptyMap();

        String inputFormat = (p.getSd() != null) ? p.getSd().getInputFormat() : null;
        String outputFormat = (p.getSd() != null) ? p.getSd().getOutputFormat() : null;
        String serdeLib = (p.getSd() != null && p.getSd().getSerdeInfo() != null)
                ? p.getSd().getSerdeInfo().getSerializationLib() : null;

        return new HmsPartitionDto(
                p.getCatName() != null ? p.getCatName() : "hive",
                p.getDbName(),
                p.getTableName(),
                p.getValues() != null ? new ArrayList<>(p.getValues()) : Collections.emptyList(),
                location,
                p.getParameters() != null ? new HashMap<>(p.getParameters()) : Collections.emptyMap(),
                cols,
                serdeParams,
                inputFormat,
                outputFormat,
                serdeLib
        );
    }

    public static Partition toThrift(HmsPartitionDto dto) {
        Partition p = new Partition();
        p.setCatName(dto.catName() != null ? dto.catName() : "hive");
        p.setDbName(dto.dbName());
        p.setTableName(dto.tableName());

        if (dto.values() != null) {
            p.setValues(new ArrayList<>(dto.values()));
        }
        if (dto.parameters() != null) {
            p.setParameters(new HashMap<>(dto.parameters()));
        }

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(dto.location());
        // Формат хранения берется у партиции источника; текстовые значения — только дефолт
        // для старых агентов, которые еще не передают формат в контракте.
        sd.setInputFormat(firstNonBlank(dto.inputFormat(), DEFAULT_PARTITION_INPUT_FORMAT));
        sd.setOutputFormat(firstNonBlank(dto.outputFormat(), DEFAULT_PARTITION_OUTPUT_FORMAT));

        SerDeInfo serde = new SerDeInfo();
        serde.setName(dto.tableName());
        serde.setSerializationLib(firstNonBlank(dto.serdeLib(), DEFAULT_PARTITION_SERDE_LIB));
        if (dto.serdeParameters() != null && !dto.serdeParameters().isEmpty()) {
            serde.setParameters(new HashMap<>(dto.serdeParameters()));
        }
        sd.setSerdeInfo(serde);

        List<FieldSchema> cols = new ArrayList<>();
        if (dto.columns() != null && !dto.columns().isEmpty()) {
            for (HmsFieldSchemaDto col : dto.columns()) {
                cols.add(new FieldSchema(col.name(), col.type(), col.comment()));
            }
        }
        sd.setCols(cols);
        p.setSd(sd);

        return p;
    }
}
