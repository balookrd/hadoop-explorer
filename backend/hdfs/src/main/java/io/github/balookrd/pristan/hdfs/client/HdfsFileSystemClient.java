package io.github.balookrd.pristan.hdfs.client;

import io.github.balookrd.pristan.hdfs.dto.file.HdfsFileStatus;
import java.io.Closeable;
import java.io.InputStream;
import java.util.List;

public interface HdfsFileSystemClient extends Closeable {

    List<HdfsFileStatus> listStatus(String path, String doAsUser);

    HdfsFileStatus getFileStatus(String path, String doAsUser);

    InputStream open(String path, String doAsUser, long offset, Long length);

    void create(String path, InputStream data, String doAsUser, boolean overwrite);

    boolean mkdirs(String path, String doAsUser);

    boolean rename(String src, String dst, String doAsUser);

    boolean delete(String path, String doAsUser, boolean recursive);

    @Override
    default void close() {}
}
