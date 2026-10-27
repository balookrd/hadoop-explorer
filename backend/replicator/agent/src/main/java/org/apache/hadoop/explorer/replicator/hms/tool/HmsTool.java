package org.apache.hadoop.explorer.replicator.hms.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.replicator.agent.ReplicatorAgentConfig;
import org.apache.hadoop.explorer.replicator.hms.client.HmsClient;
import org.apache.hadoop.explorer.replicator.hms.client.ThriftHmsClient;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * CLI-утилита прямого управления Hive Metastore через Thrift RPC.
 * Используется в скриптах администрирования, E2E и smoke-тестах для взаимодействия
 * с реальной инфраструктурой без каких-либо mock-заглушек.
 */
public class HmsTool {

    private static final Logger log = LoggerFactory.getLogger(HmsTool.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        if (args.length == 0) {
            printUsageAndExit(1);
        }

        String command = args[0];
        ReplicatorAgentConfig config = ReplicatorAgentConfig.fromEnv();

        String thriftUris = config.getHmsThriftUris();
        if (thriftUris == null || thriftUris.isBlank()) {
            System.err.println("ОШИБКА: Не задана переменная HMS_THRIFT_URIS (например, thrift://hive-metastore-1:9083)");
            System.exit(1);
        }

        String hmsPrincipal = System.getenv("HMS_KERBEROS_PRINCIPAL");
        HmsClient client = new ThriftHmsClient(
                config.getClusterId(),
                thriftUris,
                hmsPrincipal,
                config.getKeytabPath()
        );

        try {
            switch (command) {
                case "create-db":
                case "create-database": {
                    if (args.length < 2) {
                        System.err.println("Использование: create-database <dbName> [locationUri]");
                        System.exit(1);
                    }
                    String dbName = args[1];
                    String loc = args.length > 2 ? args[2] : null;
                    client.createDatabase(dbName, loc);
                    System.out.println("База данных '" + dbName + "' успешно создана в HMS");
                    break;
                }

                case "list-databases":
                case "get-databases": {
                    List<String> dbs = client.getAllDatabases();
                    System.out.println(MAPPER.writeValueAsString(dbs));
                    break;
                }

                case "create-table": {
                    if (args.length < 3) {
                        System.err.println("Использование: create-table <dbName> <tableName> [locationUri] [isPartitioned] [partKey]");
                        System.exit(1);
                    }
                    String dbName = args[1];
                    String tableName = args[2];
                    String loc = args.length > 3 ? args[3] : null;
                    boolean isPartitioned = args.length > 4 && Boolean.parseBoolean(args[4]);
                    String partKey = args.length > 5 ? args[5] : "dt";

                    List<String> partKeys = isPartitioned ? List.of(partKey) : Collections.emptyList();
                    HmsTableDto tableDto = new HmsTableDto(
                            "hive",
                            dbName,
                            tableName,
                            "MANAGED_TABLE",
                            loc,
                            Map.of("transient_lastDdlTime", String.valueOf(System.currentTimeMillis() / 1000)),
                            partKeys,
                            "org.apache.hadoop.mapred.TextInputFormat",
                            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                    );
                    client.createTable(tableDto);
                    System.out.println("Таблица '" + dbName + "." + tableName + "' успешно создана в HMS");
                    break;
                }

                case "get-table": {
                    if (args.length < 3) {
                        System.err.println("Использование: get-table <dbName> <tableName>");
                        System.exit(1);
                    }
                    String dbName = args[1];
                    String tableName = args[2];
                    Optional<HmsTableDto> tableOpt = client.getTable(dbName, tableName);
                    if (tableOpt.isPresent()) {
                        System.out.println(MAPPER.writeValueAsString(tableOpt.get()));
                        System.exit(0);
                    } else {
                        System.err.println("Таблица '" + dbName + "." + tableName + "' не найдена");
                        System.exit(2);
                    }
                    break;
                }

                case "list-tables": {
                    if (args.length < 2) {
                        System.err.println("Использование: list-tables <dbName>");
                        System.exit(1);
                    }
                    List<String> tables = client.getAllTables(args[1]);
                    System.out.println(MAPPER.writeValueAsString(tables));
                    break;
                }

                case "add-partition": {
                    if (args.length < 5) {
                        System.err.println("Использование: add-partition <dbName> <tableName> <partValue> <location>");
                        System.exit(1);
                    }
                    String dbName = args[1];
                    String tableName = args[2];
                    String partVal = args[3];
                    String loc = args[4];

                    HmsPartitionDto part = new HmsPartitionDto(
                            "hive",
                            dbName,
                            tableName,
                            List.of(partVal),
                            loc,
                            Collections.emptyMap()
                    );
                    client.addPartitions(dbName, tableName, List.of(part));
                    System.out.println("Партиция '" + partVal + "' успешно добавлена в '" + dbName + "." + tableName + "'");
                    break;
                }

                case "generate-partitions": {
                    // Нагрузочный стенд: таблица Parquet с N партициями внутри корня таблицы,
                    // добавляемыми пакетами (по одной партиции 200k вызовов заняли бы часы)
                    if (args.length < 5) {
                        System.err.println("Использование: generate-partitions <dbName> <tableName> <tableLocation> <count> [batch]");
                        System.exit(1);
                    }
                    String dbName = args[1];
                    String tableName = args[2];
                    String loc = args[3].replaceAll("/+$", "");
                    int count = Integer.parseInt(args[4]);
                    int batch = args.length > 5 ? Integer.parseInt(args[5]) : 1000;
                    String in = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
                    String out = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
                    String serde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
                    if (client.getTable(dbName, tableName).isEmpty()) {
                        client.createTable(new HmsTableDto("hive", dbName, tableName, "EXTERNAL_TABLE", loc,
                                Map.of("EXTERNAL", "TRUE"), List.of("dt"), in, out, serde));
                    }
                    int existing = client.forEachPartitionBatch(dbName, tableName, 1000, (b, total) -> { });
                    long started = System.currentTimeMillis();
                    List<HmsPartitionDto> chunk = new java.util.ArrayList<>(batch);
                    for (int i = existing; i < count; i++) {
                        String val = String.format("p%07d", i);
                        chunk.add(new HmsPartitionDto("hive", dbName, tableName, List.of(val), loc + "/dt=" + val,
                                Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap(), in, out, serde));
                        if (chunk.size() == batch || i == count - 1) {
                            client.addPartitions(dbName, tableName, chunk);
                            chunk.clear();
                            if ((i + 1) % 10_000 == 0 || i == count - 1) {
                                System.out.printf("Добавлено %d/%d партиций (%d с)%n", i + 1, count,
                                        (System.currentTimeMillis() - started) / 1000);
                            }
                        }
                    }
                    System.out.println("Готово: в '" + dbName + "." + tableName + "' " + count + " партиций");
                    break;
                }

                case "current-event-id": {
                    System.out.println(client.getCurrentNotificationEventId());
                    break;
                }

                case "churn": {
                    // Нагрузка на журнал событий для проверки CDC: потоки добавляют партиции по
                    // одной (каждая — отдельное событие ADD_PARTITION) в таблицы N баз и время от
                    // времени удаляют добавленные раньше (DROP_PARTITION)
                    if (args.length < 7) {
                        System.err.println("Использование: churn <dbPrefix> <dbCount> <tableName> <warehouseRoot> <seconds> <threads> [dropEvery] [partitionsPerEvent]");
                        System.exit(1);
                    }
                    String prefix = args[1];
                    int dbCount = Integer.parseInt(args[2]);
                    String tableName = args[3];
                    String root = args[4].replaceAll("/+$", "");
                    long deadline = System.currentTimeMillis() + Long.parseLong(args[5]) * 1000L;
                    int threads = Integer.parseInt(args[6]);
                    int dropEvery = args.length > 7 ? Integer.parseInt(args[7]) : 10;
                    // Пачка add_partitions — одно событие ADD_PARTITION на все партиции пачки
                    int perEvent = args.length > 8 ? Math.max(1, Integer.parseInt(args[8])) : 1;
                    String in = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
                    String out = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
                    String serde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
                    HmsClient pooled = new ThriftHmsClient(config.getClusterId(), thriftUris, hmsPrincipal,
                            config.getKeytabPath(), 2 * threads + 2);
                    java.util.concurrent.atomic.AtomicLong adds = new java.util.concurrent.atomic.AtomicLong();
                    java.util.concurrent.atomic.AtomicLong partitions = new java.util.concurrent.atomic.AtomicLong();
                    java.util.concurrent.atomic.AtomicLong drops = new java.util.concurrent.atomic.AtomicLong();
                    java.util.concurrent.atomic.AtomicLong errors = new java.util.concurrent.atomic.AtomicLong();
                    long firstEvent = pooled.getCurrentNotificationEventId();
                    long started = System.currentTimeMillis();
                    java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
                    for (int t = 0; t < threads; t++) {
                        final int thread = t;
                        pool.submit(() -> {
                            java.util.ArrayDeque<String[]> added = new java.util.ArrayDeque<>();
                            for (long seq = 0; System.currentTimeMillis() < deadline; seq++) {
                                String db = prefix + ((seq * threads + thread) % dbCount + 1);
                                List<HmsPartitionDto> batch = new java.util.ArrayList<>(perEvent);
                                for (int k = 0; k < perEvent; k++) {
                                    String val = String.format("c%02d_%07d_%03d", thread, seq, k);
                                    batch.add(new HmsPartitionDto("hive", db, tableName, List.of(val),
                                            root + "/" + db + ".db/" + tableName + "/dt=" + val, Collections.emptyMap(),
                                            Collections.emptyList(), Collections.emptyMap(), in, out, serde));
                                }
                                try {
                                    pooled.addPartitions(db, tableName, batch);
                                    adds.incrementAndGet();
                                    partitions.addAndGet(perEvent);
                                    added.add(new String[]{db, batch.get(0).values().get(0)});
                                    if (dropEvery > 0 && seq % dropEvery == dropEvery - 1 && added.size() > 1) {
                                        String[] victim = added.pollFirst();
                                        pooled.dropPartition(victim[0], tableName, List.of(victim[1]), false);
                                        drops.incrementAndGet();
                                    }
                                } catch (Exception e) {
                                    if (errors.incrementAndGet() <= 5) {
                                        System.err.println("Ошибка нагрузки: " + e.getMessage());
                                    }
                                }
                            }
                        });
                    }
                    pool.shutdown();
                    pool.awaitTermination(1, java.util.concurrent.TimeUnit.DAYS);
                    double secs = (System.currentTimeMillis() - started) / 1000.0;
                    long lastEvent = pooled.getCurrentNotificationEventId();
                    System.out.printf(java.util.Locale.ROOT,
                            "{\"adds\":%d,\"partitions\":%d,\"drops\":%d,\"errors\":%d,\"first_event\":%d,\"last_event\":%d,\"seconds\":%.1f,\"events_per_sec\":%.1f}%n",
                            adds.get(), partitions.get(), drops.get(), errors.get(), firstEvent, lastEvent, secs, (lastEvent - firstEvent) / secs);
                    break;
                }

                case "fire-insert": {
                    // Событие INSERT от имени DML-клиента (как HiveServer2 после записи файлов)
                    if (args.length < 6) {
                        System.err.println("Использование: fire-insert <dbName> <tableName> <partValue|-> <replace true|false> <file1,file2,...>");
                        System.exit(1);
                    }
                    List<String> partVals = "-".equals(args[3]) ? List.of() : List.of(args[3]);
                    boolean replace = Boolean.parseBoolean(args[4]);
                    List<String> files = List.of(args[5].split(","));
                    ((ThriftHmsClient) client).fireInsertEvent(args[1], args[2], partVals, files, replace);
                    System.out.println("Событие INSERT отправлено: " + args[1] + "." + args[2] + " " + partVals
                            + " replace=" + replace + " files=" + files.size());
                    break;
                }

                case "count-partitions": {
                    if (args.length < 3) {
                        System.err.println("Использование: count-partitions <dbName> <tableName>");
                        System.exit(1);
                    }
                    int[] parquet = {0};
                    int total = client.forEachPartitionBatch(args[1], args[2], 1000, (b, t) -> {
                        for (HmsPartitionDto p : b) {
                            if (p.inputFormat() != null && p.inputFormat().contains("Parquet")) {
                                parquet[0]++;
                            }
                        }
                    });
                    System.out.println("{\"total\":" + total + ",\"parquet\":" + parquet[0] + "}");
                    break;
                }

                case "get-partitions":
                case "list-partitions": {
                    if (args.length < 3) {
                        System.err.println("Использование: list-partitions <dbName> <tableName>");
                        System.exit(1);
                    }
                    List<HmsPartitionDto> parts = client.getPartitions(args[1], args[2]);
                    System.out.println(MAPPER.writeValueAsString(parts));
                    break;
                }

                case "drop-partition": {
                    if (args.length < 4) {
                        System.err.println("Использование: drop-partition <dbName> <tableName> <partValue>");
                        System.exit(1);
                    }
                    String dbName = args[1];
                    String tableName = args[2];
                    String partVal = args[3];
                    client.dropPartition(dbName, tableName, List.of(partVal), false);
                    System.out.println("Партиция '" + partVal + "' успешно удалена из '" + dbName + "." + tableName + "'");
                    break;
                }

                case "drop-table": {
                    if (args.length < 3) {
                        System.err.println("Использование: drop-table <dbName> <tableName> [deleteData]");
                        System.exit(1);
                    }
                    String dbName = args[1];
                    String tableName = args[2];
                    boolean deleteData = args.length > 3 && Boolean.parseBoolean(args[3]);
                    client.dropTable(dbName, tableName, deleteData);
                    System.out.println("Таблица '" + dbName + "." + tableName + "' успешно удалена из HMS");
                    break;
                }

                case "check-path": {
                    if (args.length < 2) {
                        System.err.println("Использование: check-path <path>");
                        System.exit(1);
                    }
                    String p = args[1];
                    org.apache.hadoop.explorer.replicator.fs.HadoopFsManager fsMgr =
                            new org.apache.hadoop.explorer.replicator.fs.HadoopFsManager(
                                    System.getenv("HDFS_DEFAULT_FS"),
                                    config.getKeytabPath(),
                                    config.getPrincipal()
                            );
                    boolean ex = fsMgr.exists(p, null, true);
                    System.out.println("RESULT: Path '" + p + "' exists=" + ex);
                    break;
                }

                default:
                    System.err.println("Неизвестная команда: " + command);
                    printUsageAndExit(1);
            }
        } catch (Exception e) {
            System.err.println("ОШИБКА выполнения команды " + command + ": " + e.getMessage());
            log.error("ОШИБКА выполнения команды {}: {}", command, e.getMessage(), e);
            System.exit(3);
        }
    }

    private static void printUsageAndExit(int code) {
        System.out.println("Использование: HmsTool <команда> [аргументы]");
        System.out.println("Команды:");
        System.out.println("  create-database <dbName> [locationUri]");
        System.out.println("  create-table <dbName> <tableName> [locationUri] [isPartitioned] [partKey]");
        System.out.println("  get-table <dbName> <tableName>");
        System.out.println("  list-tables <dbName>");
        System.out.println("  add-partition <dbName> <tableName> <partValue> <location>");
        System.out.println("  list-partitions <dbName> <tableName>");
        System.out.println("  drop-partition <dbName> <tableName> <partValue>");
        System.out.println("  drop-table <dbName> <tableName> [deleteData]");
        System.out.println("  current-event-id");
        System.out.println("  churn <dbPrefix> <dbCount> <tableName> <warehouseRoot> <seconds> <threads> [dropEvery] [partitionsPerEvent]");
        System.exit(code);
    }
}
