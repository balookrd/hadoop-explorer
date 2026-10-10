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
        System.exit(code);
    }
}
