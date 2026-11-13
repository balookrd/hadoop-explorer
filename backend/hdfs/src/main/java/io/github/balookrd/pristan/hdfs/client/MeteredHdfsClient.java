package io.github.balookrd.pristan.hdfs.client;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.github.balookrd.pristan.hdfs.dto.file.HdfsFileStatus;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.function.Supplier;

/**
 * Counts HDFS client calls and transferred bytes per cluster for Prometheus.
 * Metric names match monitoring/grafana/dashboards/hdfs_explorer_operations.json.
 */
public class MeteredHdfsClient implements HdfsFileSystemClient {

    public static final String OPERATIONS_METRIC = "hdfs.operations";
    public static final String BYTES_METRIC = "hdfs.bytes.transferred";

    private final HdfsFileSystemClient delegate;
    private final MeterRegistry registry;
    private final String clusterId;
    private final Counter uploadedBytes;
    private final Counter downloadedBytes;

    public MeteredHdfsClient(HdfsFileSystemClient delegate, MeterRegistry registry, String clusterId) {
        this.delegate = delegate;
        this.registry = registry;
        this.clusterId = clusterId;
        this.uploadedBytes = bytesCounter("upload");
        this.downloadedBytes = bytesCounter("download");
    }

    /** The wrapped client, for code paths that need a concrete implementation. */
    public HdfsFileSystemClient delegate() {
        return delegate;
    }

    @Override
    public List<HdfsFileStatus> listStatus(String path, String doAsUser) {
        return track("list_status", () -> delegate.listStatus(path, doAsUser));
    }

    @Override
    public HdfsFileStatus getFileStatus(String path, String doAsUser) {
        return track("get_file_status", () -> delegate.getFileStatus(path, doAsUser));
    }

    @Override
    public InputStream open(String path, String doAsUser, long offset, Long length) {
        InputStream in = track("open", () -> delegate.open(path, doAsUser, offset, length));
        return new CountingInputStream(in, downloadedBytes);
    }

    @Override
    public void create(String path, InputStream data, String doAsUser, boolean overwrite) {
        track("create", () -> {
            delegate.create(path, new CountingInputStream(data, uploadedBytes), doAsUser, overwrite);
            return null;
        });
    }

    @Override
    public boolean mkdirs(String path, String doAsUser) {
        return track("mkdirs", () -> delegate.mkdirs(path, doAsUser));
    }

    @Override
    public boolean rename(String src, String dst, String doAsUser) {
        return track("rename", () -> delegate.rename(src, dst, doAsUser));
    }

    @Override
    public boolean delete(String path, String doAsUser, boolean recursive) {
        return track("delete", () -> delegate.delete(path, doAsUser, recursive));
    }

    @Override
    public void close() {
        delegate.close();
    }

    private <T> T track(String operation, Supplier<T> call) {
        try {
            T result = call.get();
            operationCounter(operation, "success").increment();
            return result;
        } catch (RuntimeException e) {
            operationCounter(operation, "failed").increment();
            throw e;
        }
    }

    private Counter operationCounter(String operation, String status) {
        return Counter.builder(OPERATIONS_METRIC)
            .description("HDFS client calls made by HDFS Explorer")
            .tag("cluster", clusterId)
            .tag("operation", operation)
            .tag("status", status)
            .register(registry);
    }

    private Counter bytesCounter(String direction) {
        // No baseUnit: Micrometer would rename the series to hdfs_bytes_transferred_bytes_total
        return Counter.builder(BYTES_METRIC)
            .description("Bytes read from or written to HDFS by HDFS Explorer")
            .tag("cluster", clusterId)
            .tag("direction", direction)
            .register(registry);
    }

    private static final class CountingInputStream extends FilterInputStream {

        private final Counter counter;

        CountingInputStream(InputStream in, Counter counter) {
            super(in);
            this.counter = counter;
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b >= 0) {
                counter.increment();
            }
            return b;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int n = super.read(buffer, offset, length);
            if (n > 0) {
                counter.increment(n);
            }
            return n;
        }
    }
}
