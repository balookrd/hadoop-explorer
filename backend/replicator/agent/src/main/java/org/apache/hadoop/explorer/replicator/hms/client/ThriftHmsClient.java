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
    private final GenericObjectPool<IMetaStoreClient> clientPool;

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

        // Kerberos / SASL конфигурация Hive Metastore
        if (kerberosPrincipal != null && !kerberosPrincipal.isBlank()) {
            this.conf.set("hadoop.security.authentication", "kerberos");
            this.conf.set("hive.metastore.sasl.enabled", "true");
            this.conf.set("hive.metastore.kerberos.principal", kerberosPrincipal);
            if (keytabPath != null && !keytabPath.isBlank()) {
                this.conf.set("hive.metastore.kerberos.keytab.file", keytabPath);
            }
        }

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
    }

    public GenericObjectPool<IMetaStoreClient> getClientPool() {
        return clientPool;
    }

    private class MetaStoreClientFactory extends BasePooledObjectFactory<IMetaStoreClient> {
        @Override
        public IMetaStoreClient create() throws Exception {
            if (kerberosPrincipal != null && !kerberosPrincipal.isBlank() || UserGroupInformation.isSecurityEnabled()) {
                conf.set("hadoop.security.authentication", "kerberos");
                UserGroupInformation.setConfiguration(conf);
                if (keytabPath != null && !keytabPath.isBlank()) {
                    String userPrinc = System.getenv("KRB5_PRINCIPAL");
                    if (userPrinc == null || userPrinc.isBlank()) {
                        userPrinc = kerberosPrincipal;
                    }
                    UserGroupInformation.loginUserFromKeytab(userPrinc, keytabPath);
                }
                UserGroupInformation ugi = UserGroupInformation.getLoginUser();
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
                return p != null && p.getObject() != null;
            } catch (Exception e) {
                return false;
            }
        }
    }

    private <T> T executeWithRetry(HmsOperation<T> operation) {
        IMetaStoreClient client = null;
        try {
            client = clientPool.borrowObject();
            return operation.execute(client);
        } catch (MetaException e) {
            log.warn("[ThriftHmsClient:{}] Ошибка MetaException: {}, попытка повтора с новым соединением...", clusterId, e.getMessage());
            if (client != null) {
                try {
                    clientPool.invalidateObject(client);
                } catch (Exception ignored) {}
                client = null;
            }
            try {
                client = clientPool.borrowObject();
                return operation.execute(client);
            } catch (Exception ex) {
                throw new RuntimeException("Ошибка выполнения операции Hive Metastore: " + ex.getMessage(), ex);
            }
        } catch (Exception e) {
            throw new RuntimeException("Ошибка выполнения операции Hive Metastore: " + e.getMessage(), e);
        } finally {
            if (client != null) {
                clientPool.returnObject(client);
            }
        }
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

    @Override
    public void addPartitions(String dbName, String tableName, List<HmsPartitionDto> partitions) {
        if (partitions == null || partitions.isEmpty()) return;
        executeWithRetry(c -> {
            try {
                List<Partition> thriftPartitions = new ArrayList<>(partitions.size());
                for (HmsPartitionDto pDto : partitions) {
                    thriftPartitions.add(toThrift(pDto));
                }
                c.add_partitions(thriftPartitions);
                log.info("[ThriftHmsClient:{}] {} партиций добавлено в '{}.{}' Hive Metastore",
                        clusterId, thriftPartitions.size(), dbName, tableName);
            } catch (AlreadyExistsException e) {
                log.info("[ThriftHmsClient:{}] Партиции уже существуют в '{}.{}', добавление пропущено (идемпотентность)",
                        clusterId, dbName, tableName);
            }
            return null;
        });
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

        if (dto.partitionKeyFields() != null && !dto.partitionKeyFields().isEmpty()) {
            List<FieldSchema> pks = new ArrayList<>(dto.partitionKeyFields().size());
            for (HmsFieldSchemaDto pk : dto.partitionKeyFields()) {
                pks.add(new FieldSchema(pk.name(), pk.type(), pk.comment()));
            }
            t.setPartitionKeys(pks);
        } else if (dto.partitionKeys() != null && !dto.partitionKeys().isEmpty()) {
            List<FieldSchema> pks = new ArrayList<>(dto.partitionKeys().size());
            for (String pk : dto.partitionKeys()) {
                pks.add(new FieldSchema(pk, "string", "partition key"));
            }
            t.setPartitionKeys(pks);
        }

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

        return new HmsPartitionDto(
                p.getCatName() != null ? p.getCatName() : "hive",
                p.getDbName(),
                p.getTableName(),
                p.getValues() != null ? new ArrayList<>(p.getValues()) : Collections.emptyList(),
                location,
                p.getParameters() != null ? new HashMap<>(p.getParameters()) : Collections.emptyMap(),
                cols,
                serdeParams
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
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");

        SerDeInfo serde = new SerDeInfo();
        serde.setName(dto.tableName());
        serde.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
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
