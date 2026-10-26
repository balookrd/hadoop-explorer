package org.apache.hadoop.explorer.replicator.fs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

/**
 * Файловая система, которая отвечает отказом на любой вопрос о состоянии пути.
 *
 * <p>Имитирует то, что в проде выглядит как истекший тикет Kerberos или NameNode в safemode:
 * клиент не знает, есть файл или нет. Регистрируется в тесте через {@code fs.failing.impl}.
 */
public class FailingFileSystem extends FileSystem {

    public static final String SCHEME = "failing";
    public static final String FAILURE_MESSAGE = "Kerberos ticket expired";

    private URI uri = URI.create(SCHEME + ":///");

    @Override
    public void initialize(URI name, org.apache.hadoop.conf.Configuration conf) throws IOException {
        super.initialize(name, conf);
        this.uri = name;
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        throw new IOException(FAILURE_MESSAGE);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
        throw new IOException(FAILURE_MESSAGE);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        throw new IOException(FAILURE_MESSAGE);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress) throws IOException {
        throw new IOException(FAILURE_MESSAGE);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new IOException(FAILURE_MESSAGE);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new IOException(FAILURE_MESSAGE);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        throw new IOException(FAILURE_MESSAGE);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        throw new IOException(FAILURE_MESSAGE);
    }

    @Override
    public void setWorkingDirectory(Path newDir) {
    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(uri);
    }
}
