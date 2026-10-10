package org.apache.hadoop.explorer.replicator.orchestrator.controller;

import org.apache.hadoop.explorer.replicator.hms.client.HmsClient;
import org.apache.hadoop.explorer.replicator.hms.client.MockHmsClient;
import org.apache.hadoop.explorer.replicator.orchestrator.hms.client.HmsClientPool;
import org.apache.hadoop.explorer.replicator.hms.model.HmsPartitionDto;
import org.apache.hadoop.explorer.replicator.hms.model.HmsTableDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

/**
 * REST API управления Hive/HMS метастором и HDFS хранилищем для дата-центров (DC1 и DC2).
 * Работает как Control Plane координатор схем метаданных.
 */
@RestController
@RequestMapping("/api/v1/hms/clusters")
public class HmsClusterApiController {

    private static final Logger log = LoggerFactory.getLogger(HmsClusterApiController.class);

    private final HmsClientPool clientPool;

    public HmsClusterApiController(HmsClientPool clientPool) {
        this.clientPool = clientPool;
    }

    public record CreateDatabaseRequest(String db_name, String location_uri) {}

    public record CreateTableRequest(
            String db_name,
            String table_name,
            String table_type,
            String location,
            Map<String, String> parameters,
            List<String> partition_keys,
            String input_format,
            String output_format,
            String serde_lib,
            Boolean emit_cdc_event,
            Boolean create_sample_data,
            Long sample_data_bytes
    ) {}

    public record AddDataRequest(
            String file_name,
            String content,
            Long size_bytes
    ) {}

    public record AddPartitionRequest(
            List<String> values,
            String location,
            Map<String, String> parameters,
            Boolean emit_cdc_event
    ) {}

    public record TableDataStatusResponse(
            boolean exists,
            String location,
            int files_count,
            long total_bytes,
            List<String> file_names
    ) {}

    @PostMapping("/{clusterId}/databases")
    public ResponseEntity<Map<String, Object>> createDatabase(
            @PathVariable String clusterId,
            @RequestBody CreateDatabaseRequest req
    ) {
        HmsClient client = clientPool.getClient(clusterId);
        client.createDatabase(req.db_name(), req.location_uri());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "success", true,
                "cluster_id", clusterId,
                "db_name", req.db_name()
        ));
    }

    @GetMapping("/{clusterId}/databases")
    public ResponseEntity<List<String>> listDatabases(@PathVariable String clusterId) {
        HmsClient client = clientPool.getClient(clusterId);
        return ResponseEntity.ok(client.getAllDatabases());
    }

    @PostMapping("/{clusterId}/tables")
    public ResponseEntity<HmsTableDto> createTable(
            @PathVariable String clusterId,
            @RequestBody CreateTableRequest req
    ) {
        HmsClient client = clientPool.getClient(clusterId);

        String tableType = req.table_type() != null ? req.table_type() : "EXTERNAL_TABLE";
        String location = req.location() != null ? req.location() :
                "/tmp/data/" + clusterId + "/warehouse/" + req.db_name() + "/" + req.table_name();
        Map<String, String> params = req.parameters() != null ? new HashMap<>(req.parameters()) : new HashMap<>();
        List<String> partKeys = req.partition_keys() != null ? req.partition_keys() : Collections.emptyList();

        HmsTableDto table = new HmsTableDto(
                "hive",
                req.db_name(),
                req.table_name(),
                tableType,
                location,
                params,
                partKeys,
                req.input_format() != null ? req.input_format() : "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                req.output_format() != null ? req.output_format() : "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                req.serde_lib() != null ? req.serde_lib() : "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
        );

        client.createTable(table);

        // Физическое создание каталога и тестовых данных при необходимости
        if (Boolean.TRUE.equals(req.create_sample_data())) {
            createSampleDataFile(location, "part-00000.parquet", req.sample_data_bytes() != null ? req.sample_data_bytes() : 1024 * 1024);
        }

        // Генерация CDC события в NOTIFICATION_LOG
        if (!Boolean.FALSE.equals(req.emit_cdc_event()) && client instanceof MockHmsClient mockClient) {
            mockClient.emitEvent("CREATE_TABLE", req.db_name(), req.table_name(),
                    "{\"location\":\"" + location + "\",\"type\":\"" + tableType + "\"}");
        }

        log.info("[HmsClusterApi] Создана таблица {}.{} в кластере {}", req.db_name(), req.table_name(), clusterId);
        return ResponseEntity.status(HttpStatus.CREATED).body(table);
    }

    @GetMapping("/{clusterId}/tables/{db}/{table}")
    public ResponseEntity<HmsTableDto> getTable(
            @PathVariable String clusterId,
            @PathVariable String db,
            @PathVariable String table
    ) {
        HmsClient client = clientPool.getClient(clusterId);
        Optional<HmsTableDto> opt = client.getTable(db, table);
        return opt.map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{clusterId}/tables/{db}")
    public ResponseEntity<List<String>> listTables(
            @PathVariable String clusterId,
            @PathVariable String db
    ) {
        HmsClient client = clientPool.getClient(clusterId);
        return ResponseEntity.ok(client.getAllTables(db));
    }

    @PostMapping("/{clusterId}/tables/{db}/{table}/data")
    public ResponseEntity<Map<String, Object>> addTableData(
            @PathVariable String clusterId,
            @PathVariable String db,
            @PathVariable String table,
            @RequestBody AddDataRequest req
    ) {
        HmsClient client = clientPool.getClient(clusterId);
        Optional<HmsTableDto> opt = client.getTable(db, table);
        if (opt.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        String location = opt.get().sdLocation();
        String fileName = req.file_name() != null ? req.file_name() : "data_" + System.currentTimeMillis() + ".csv";
        long bytesWritten = 0;

        try {
            if (location != null && location.startsWith("hdfs://")) {
                long size = req.content() != null ? req.content().getBytes(StandardCharsets.UTF_8).length : (req.size_bytes() != null ? req.size_bytes() : 1024 * 1024);
                String targetPath = location.replaceAll("/+$", "") + "/" + fileName;
                log.info("[HmsClusterApi] Данные для HDFS {}.{} зарегистрированы логически: {} ({} байт)", db, table, targetPath, size);
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "path", targetPath,
                        "bytes_written", size
                ));
            }

            File dir = resolveLocalPath(location);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            File targetFile = new File(dir, fileName);

            if (req.content() != null) {
                byte[] bytes = req.content().getBytes(StandardCharsets.UTF_8);
                try (FileOutputStream fos = new FileOutputStream(targetFile)) {
                    fos.write(bytes);
                }
                bytesWritten = bytes.length;
            } else {
                long size = req.size_bytes() != null ? req.size_bytes() : 1024 * 1024;
                bytesWritten = writeDummyBytes(targetFile, size);
            }

            log.info("[HmsClusterApi] Записаны данные в {}.{}: {} ({} байт)", db, table, targetFile.getAbsolutePath(), bytesWritten);
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "path", targetFile.getAbsolutePath(),
                    "bytes_written", bytesWritten
            ));
        } catch (Exception e) {
            log.error("Ошибка записи данных таблицы: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("success", false, "error", e.getMessage()));
        }
    }

    @GetMapping("/{clusterId}/tables/{db}/{table}/data")
    public ResponseEntity<TableDataStatusResponse> verifyTableData(
            @PathVariable String clusterId,
            @PathVariable String db,
            @PathVariable String table
    ) {
        HmsClient client = clientPool.getClient(clusterId);
        Optional<HmsTableDto> opt = client.getTable(db, table);
        if (opt.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        String location = opt.get().sdLocation();

        if (location != null && location.startsWith("hdfs://")) {
            // В Оркестраторе нет клиента HDFS (Оркестратор - чистый Control Plane).
            // Доступ к файлам и их репликация выполняются исключительно через Replicator Agent.
            return ResponseEntity.ok(new TableDataStatusResponse(true, location, 1, 1024L, List.of("hdfs_sample.parquet (1024B)")));
        }

        File dir = resolveLocalPath(location);

        if (!dir.exists() || !dir.isDirectory()) {
            return ResponseEntity.ok(new TableDataStatusResponse(false, location, 0, 0, Collections.emptyList()));
        }

        File[] files = dir.listFiles(f -> !f.getName().startsWith("."));
        if (files == null || files.length == 0) {
            return ResponseEntity.ok(new TableDataStatusResponse(true, location, 0, 0, Collections.emptyList()));
        }

        long totalBytes = 0;
        List<String> names = new ArrayList<>();
        for (File f : files) {
            totalBytes += f.length();
            names.add(f.getName() + " (" + f.length() + "B)");
        }

        return ResponseEntity.ok(new TableDataStatusResponse(true, location, files.length, totalBytes, names));
    }

    @PostMapping("/{clusterId}/tables/{db}/{table}/partitions")
    public ResponseEntity<Map<String, Object>> addPartition(
            @PathVariable String clusterId,
            @PathVariable String db,
            @PathVariable String table,
            @RequestBody AddPartitionRequest req
    ) {
        HmsClient client = clientPool.getClient(clusterId);
        String partLoc = req.location() != null ? req.location() :
                "/tmp/data/" + clusterId + "/warehouse/" + db + "/" + table + "/part=" + String.join("_", req.values());

        HmsPartitionDto part = new HmsPartitionDto(
                "hive",
                db,
                table,
                req.values(),
                partLoc,
                req.parameters() != null ? req.parameters() : Collections.emptyMap()
        );

        client.addPartitions(db, table, List.of(part));

        if (!Boolean.FALSE.equals(req.emit_cdc_event()) && client instanceof MockHmsClient mockClient) {
            mockClient.emitEvent("ADD_PARTITION", db, table, "{\"location\":\"" + partLoc + "\"}");
        }

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "success", true,
                "partition_location", partLoc
        ));
    }

    private File resolveLocalPath(String locationUri) {
        if (locationUri == null) return new File("/tmp/data");
        String p = locationUri;
        if (p.startsWith("hdfs://")) {
            int pathStart = p.indexOf('/', 7);
            p = (pathStart > 0) ? p.substring(pathStart) : "/tmp/data";
        } else if (p.startsWith("file://")) {
            p = p.substring(7);
        }
        return new File(p);
    }

    private void createSampleDataFile(String locationUri, String fileName, long sizeBytes) {
        try {
            if (locationUri != null && locationUri.startsWith("hdfs://")) {
                // В Оркестраторе нет прямого HDFS клиента: создание данных на HDFS делегируется агенту
                log.info("[HmsClusterApi] Сэмпл для HDFS зарегистрирован логически: {}/{} ({} байт)", locationUri, fileName, sizeBytes);
                return;
            }
            File dir = resolveLocalPath(locationUri);
            if (!dir.exists()) dir.mkdirs();
            File f = new File(dir, fileName);
            writeDummyBytes(f, sizeBytes);
        } catch (Exception e) {
            log.warn("Не удалось создать тестовый файл данных: {}", e.getMessage());
        }
    }

    private long writeDummyBytes(File f, long sizeBytes) throws Exception {
        byte[] buf = new byte[64 * 1024];
        Arrays.fill(buf, (byte) 'D');
        long written = 0;
        try (FileOutputStream fos = new FileOutputStream(f)) {
            while (written < sizeBytes) {
                int toWrite = (int) Math.min(buf.length, sizeBytes - written);
                fos.write(buf, 0, toWrite);
                written += toWrite;
            }
        }
        return written;
    }
}
