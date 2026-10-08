package org.apache.hadoop.explorer.hdfs.service;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.explorer.hdfs.client.HdfsFileSystemClient;
import org.apache.hadoop.explorer.hdfs.client.NativeHdfsClient;
import org.apache.hadoop.explorer.hdfs.dto.file.FilePreviewResponse;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Service
public class FilePreviewService {

    private static final Logger log = LoggerFactory.getLogger(FilePreviewService.class);

    private static final Set<String> TEXT_EXTENSIONS = Set.of(
        "txt", "log", "json", "xml", "yaml", "yml", "md", "sql", "sh", "py",
        "java", "properties", "conf", "config", "env", "ini", "toml"
    );

    /**
     * Высокоуровневый метод предварительного просмотра файла через HdfsFileSystemClient.
     * Для бинарных колоночных форматов (ORC, Parquet) использует прямое чтение метаданных
     * и первых строк через RPC/HDFS без скачивания всего файла на сервер.
     */
    public FilePreviewResponse preview(HdfsFileSystemClient client, String clusterId, String path,
                                       String username, long totalSize, int maxBytes) {
        String ext = getExtension(path).toLowerCase();
        boolean isOrc = ext.equals("orc");
        boolean isParquet = ext.equals("parquet") || ext.equals("parq");

        if (isOrc || isParquet) {
            if (client instanceof NativeHdfsClient nativeClient) {
                return nativeClient.executeWithFileSystem(username, (fs, conf) -> {
                    Path hdfsPath = new Path(path);
                    if (isOrc) {
                        return parseOrc(clusterId, path, hdfsPath, conf, fs, totalSize);
                    } else {
                        return parseParquet(clusterId, path, hdfsPath, conf, fs, totalSize);
                    }
                });
            } else {
                try (InputStream stream = client.open(path, username, 0, null)) {
                    return parseColumnarFromStream(clusterId, path, stream, totalSize, isOrc);
                } catch (Exception e) {
                    log.error("Failed to read columnar file {} via stream", path, e);
                    return new FilePreviewResponse(
                        clusterId, path, isOrc ? "orc" : "parquet", totalSize, false,
                        null, null, null, 0, "Ошибка чтения предпросмотра: " + e.getMessage()
                    );
                }
            }
        }

        // Для текстовых и CSV файлов читаем поток до лимита maxBytes
        try (InputStream stream = client.open(path, username, 0, (long) maxBytes)) {
            return generatePreview(clusterId, path, stream, totalSize, maxBytes);
        } catch (IOException e) {
            log.error("Failed to read file preview for {}", path, e);
            return new FilePreviewResponse(
                clusterId, path, ext.isBlank() ? "unknown" : ext, totalSize, false,
                null, null, null, null, "Ошибка чтения предпросмотра: " + e.getMessage()
            );
        }
    }

    /**
     * Потоковый метод предпросмотра (подходит для CSV, JSON, TXT и тестирования).
     */
    public FilePreviewResponse generatePreview(String clusterId, String path, InputStream stream,
                                               long totalSize, int maxBytes) {
        String ext = getExtension(path).toLowerCase();
        boolean isCsv = ext.equals("csv") || ext.equals("tsv");
        boolean isText = isCsv || TEXT_EXTENSIONS.contains(ext);
        boolean isParquet = ext.equals("parquet") || ext.equals("parq");
        boolean isOrc = ext.equals("orc");

        int limit = Math.min((int) totalSize, maxBytes);
        boolean truncated = totalSize > limit;

        if (isCsv) {
            return generateCsvPreview(clusterId, path, stream, totalSize, ext.equals("tsv") ? "\t" : ",", truncated);
        }

        if (isParquet || isOrc) {
            return parseColumnarFromStream(clusterId, path, stream, totalSize, isOrc);
        }

        if (isText || isProbableText(ext)) {
            try {
                byte[] buffer = new byte[limit];
                int read = stream.readNBytes(buffer, 0, limit);
                String content = new String(buffer, 0, read, StandardCharsets.UTF_8);
                return new FilePreviewResponse(
                    clusterId, path, ext.isBlank() ? "text" : ext, totalSize, truncated,
                    content, null, null, null, null
                );
            } catch (Exception e) {
                log.error("Failed to read text preview for {}", path, e);
                return new FilePreviewResponse(
                    clusterId, path, "unknown", totalSize, false,
                    null, null, null, null, "Ошибка чтения предпросмотра: " + e.getMessage()
                );
            }
        }

        // Двоичный файл без поддержки табличного предпросмотра
        return new FilePreviewResponse(
            clusterId, path, "binary", totalSize, false,
            "Бинарный файл (" + ext + "). Предпросмотр не поддерживается. Размер: " + formatSize(totalSize),
            null, null, null, null
        );
    }

    private FilePreviewResponse parseColumnarFromStream(String clusterId, String path, InputStream stream,
                                                        long totalSize, boolean isOrc) {
        File temp = null;
        try {
            String ext = isOrc ? "orc" : "parquet";
            temp = File.createTempFile("preview_", "." + ext);
            try (FileOutputStream out = new FileOutputStream(temp)) {
                stream.transferTo(out);
            }
            Configuration conf = new Configuration();
            Path localPath = new Path(temp.toURI());
            FileSystem fs = localPath.getFileSystem(conf);
            if (isOrc) {
                return parseOrc(clusterId, path, localPath, conf, fs, totalSize);
            } else {
                return parseParquet(clusterId, path, localPath, conf, fs, totalSize);
            }
        } catch (Exception e) {
            log.error("Failed to parse columnar stream for {}", path, e);
            return new FilePreviewResponse(
                clusterId, path, isOrc ? "orc" : "parquet", totalSize, false,
                null, null, null, 0, "Ошибка разбора файла: " + e.getMessage()
            );
        } finally {
            if (temp != null && temp.exists()) {
                //noinspection ResultOfMethodCallIgnored
                temp.delete();
            }
        }
    }

    private FilePreviewResponse parseOrc(String clusterId, String path, Path hdfsPath,
                                         Configuration conf, FileSystem fs, long totalSize) {
        try {
            OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
            if (fs != null) {
                options.filesystem(fs);
            }
            Reader reader = OrcFile.createReader(hdfsPath, options);
            TypeDescription schema = reader.getSchema();
            List<String> columnNames = new ArrayList<>(schema.getFieldNames());
            List<TypeDescription> children = schema.getChildren();
            long totalRows = reader.getNumberOfRows();
            int numStripes = reader.getStripes().size();

            StringBuilder schemaDesc = new StringBuilder();
            schemaDesc.append("Файл Apache ORC (размер: ").append(formatSize(totalSize))
                      .append(", строк: ").append(totalRows)
                      .append(", страйпов: ").append(numStripes).append(").\n")
                      .append("Схема колонок:\n");

            for (int i = 0; i < columnNames.size() && i < children.size(); i++) {
                schemaDesc.append("  • ").append(columnNames.get(i))
                          .append(" : ").append(children.get(i).getCategory().getName()).append("\n");
            }

            List<List<Object>> rows = new ArrayList<>();
            int maxRows = 100;
            try (RecordReader recordReader = reader.rows(new Reader.Options())) {
                VectorizedRowBatch batch = schema.createRowBatch(Math.min(maxRows, (int) Math.max(1, totalRows)));
                while (recordReader.nextBatch(batch) && rows.size() < maxRows) {
                    for (int r = 0; r < batch.size && rows.size() < maxRows; r++) {
                        List<Object> row = new ArrayList<>();
                        for (int c = 0; c < batch.numCols; c++) {
                            TypeDescription colType = (c < children.size()) ? children.get(c) : null;
                            row.add(extractOrcValue(batch.cols[c], r, colType));
                        }
                        rows.add(row);
                    }
                }
            }

            return new FilePreviewResponse(
                clusterId, path, "orc", totalSize, totalRows > rows.size(),
                schemaDesc.toString().trim(), columnNames, rows, rows.size(), null
            );
        } catch (Exception e) {
            log.error("Failed to parse ORC file {}", path, e);
            return new FilePreviewResponse(
                clusterId, path, "orc", totalSize, false,
                null, null, null, 0, "Ошибка разбора ORC файла: " + e.getMessage()
            );
        }
    }

    private Object extractOrcValue(ColumnVector col, int row, TypeDescription type) {
        if (col == null) return null;
        int idx = col.isRepeating ? 0 : row;
        if (!col.noNulls && col.isNull[idx]) {
            return null;
        }

        if (col instanceof LongColumnVector lcv) {
            long val = lcv.vector[idx];
            if (type != null && type.getCategory() == TypeDescription.Category.BOOLEAN) {
                return val != 0;
            }
            if (type != null && type.getCategory() == TypeDescription.Category.DATE) {
                return java.time.LocalDate.ofEpochDay(val).toString();
            }
            return val;
        }

        if (col instanceof DoubleColumnVector dcv) {
            return dcv.vector[idx];
        }

        if (col instanceof BytesColumnVector bcv) {
            return new String(bcv.vector[idx], bcv.start[idx], bcv.length[idx], StandardCharsets.UTF_8);
        }

        if (col instanceof DecimalColumnVector dcv) {
            return dcv.vector[idx].toString();
        }

        if (col instanceof TimestampColumnVector tcv) {
            return tcv.asScratchTimestamp(idx).toString();
        }

        return col.toString();
    }

    private FilePreviewResponse parseParquet(String clusterId, String path, Path hdfsPath,
                                             Configuration conf, FileSystem fs, long totalSize) {
        try {
            ParquetMetadata footer = ParquetFileReader.readFooter(conf, hdfsPath, ParquetMetadataConverter.NO_FILTER);
            MessageType schema = footer.getFileMetaData().getSchema();
            List<String> columnNames = new ArrayList<>();
            for (Type field : schema.getFields()) {
                columnNames.add(field.getName());
            }

            long totalRows = footer.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
            int numRowGroups = footer.getBlocks().size();

            StringBuilder schemaDesc = new StringBuilder();
            schemaDesc.append("Файл Apache Parquet (размер: ").append(formatSize(totalSize))
                      .append(", строк: ").append(totalRows)
                      .append(", групп строк: ").append(numRowGroups).append(").\n")
                      .append("Схема колонок:\n");

            for (Type field : schema.getFields()) {
                String typeName = field.isPrimitive() ? field.asPrimitiveType().getPrimitiveTypeName().name() : "GROUP";
                schemaDesc.append("  • ").append(field.getName()).append(" : ").append(typeName).append("\n");
            }

            List<List<Object>> rows = new ArrayList<>();
            int maxRows = 100;

            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(hdfsPath)
                    .withConf(conf)
                    .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null && rows.size() < maxRows) {
                    List<Object> row = new ArrayList<>();
                    for (String col : columnNames) {
                        Object val = record.get(col);
                        if (val == null) {
                            row.add(null);
                        } else if (val instanceof org.apache.avro.util.Utf8 utf8) {
                            row.add(utf8.toString());
                        } else if (val instanceof ByteBuffer bb) {
                            byte[] bytes = new byte[bb.remaining()];
                            bb.get(bytes);
                            row.add(new String(bytes, StandardCharsets.UTF_8));
                        } else {
                            row.add(val.toString());
                        }
                    }
                    rows.add(row);
                }
            }

            return new FilePreviewResponse(
                clusterId, path, "parquet", totalSize, totalRows > rows.size(),
                schemaDesc.toString().trim(), columnNames, rows, rows.size(), null
            );
        } catch (Exception e) {
            log.error("Failed to parse Parquet file {}", path, e);
            return new FilePreviewResponse(
                clusterId, path, "parquet", totalSize, false,
                null, null, null, 0, "Ошибка разбора Parquet файла: " + e.getMessage()
            );
        }
    }

    private FilePreviewResponse generateCsvPreview(String clusterId, String path, InputStream stream,
                                                   long totalSize, String delimiter, boolean truncated) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            List<String> columns = new ArrayList<>();
            List<List<Object>> rows = new ArrayList<>();
            String line;
            int rowCount = 0;

            if ((line = reader.readLine()) != null) {
                String[] cols = parseCsvLine(line, delimiter);
                columns.addAll(Arrays.asList(cols));
            }

            while ((line = reader.readLine()) != null && rowCount < 100) {
                String[] values = parseCsvLine(line, delimiter);
                List<Object> row = new ArrayList<>();
                for (String v : values) {
                    row.add(v.trim());
                }
                rows.add(row);
                rowCount++;
            }

            return new FilePreviewResponse(
                clusterId, path, delimiter.equals("\t") ? "tsv" : "csv", totalSize, truncated,
                null, columns, rows, rowCount, null
            );
        } catch (Exception e) {
            log.error("Failed to parse CSV preview for {}", path, e);
            return new FilePreviewResponse(
                clusterId, path, "csv", totalSize, false,
                null, null, null, 0, "Ошибка разбора CSV: " + e.getMessage()
            );
        }
    }

    private String[] parseCsvLine(String line, String delimiter) {
        if (delimiter.equals("\t")) {
            return line.split("\t", -1);
        }
        return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    }

    private boolean isProbableText(String ext) {
        return ext.isBlank() || ext.length() <= 4;
    }

    private String getExtension(String path) {
        if (path == null) return "";
        int dot = path.lastIndexOf('.');
        int slash = path.lastIndexOf('/');
        if (dot > slash && dot < path.length() - 1) {
            return path.substring(dot + 1);
        }
        return "";
    }

    private String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format(Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format(Locale.ROOT, "%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
}
