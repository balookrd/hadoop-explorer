package org.apache.hadoop.explorer.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.explorer.hdfs.dto.cluster.ClusterConfig;
import org.apache.hadoop.explorer.hdfs.dto.file.HdfsFileStatus;
import org.apache.hadoop.explorer.hdfs.exception.HdfsErrorTranslator;
import org.apache.hadoop.explorer.hdfs.exception.HdfsLocalizedException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

public class NativeHdfsClient implements HdfsFileSystemClient {

    private static final Logger log = LoggerFactory.getLogger(NativeHdfsClient.class);

    private final ClusterConfig clusterConfig;
    private final Configuration hadoopConf;
    private final UserGroupInformation loginUser;
    private final URI fileSystemUri;

    public NativeHdfsClient(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.hadoopConf = new Configuration();

        // 1. Настройка адресов и High Availability
        this.fileSystemUri = configureFileSystemUri(clusterConfig, hadoopConf);

        // 2. Настройка Kerberos
        if ("kerberos".equalsIgnoreCase(clusterConfig.getAuthType())) {
            hadoopConf.set("hadoop.security.authentication", "kerberos");
            hadoopConf.set("dfs.namenode.kerberos.principal.pattern", "*");
            hadoopConf.set("dfs.data.transfer.protection", "integrity");

            String nnPrincipal = clusterConfig.getNamenodePrincipal();
            if (nnPrincipal == null || nnPrincipal.isBlank()) {
                nnPrincipal = "nn/" + clusterConfig.getId() + ".demo-platform-net@COMPANY.LOCAL";
            }
            hadoopConf.set("dfs.namenode.kerberos.principal", nnPrincipal);
            log.info("Configured NameNode Kerberos principal: {}", nnPrincipal);

            UserGroupInformation.setConfiguration(hadoopConf);
            try {
                if (clusterConfig.getServicePrincipal() != null && clusterConfig.getKeytabPath() != null) {
                    log.info("Logging into Kerberos from keytab: principal={}, keytab={}",
                        clusterConfig.getServicePrincipal(), clusterConfig.getKeytabPath());
                    this.loginUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                        clusterConfig.getServicePrincipal(), clusterConfig.getKeytabPath());
                } else {
                    this.loginUser = UserGroupInformation.getLoginUser();
                }
            } catch (IOException e) {
                log.error("Failed to authenticate with Kerberos for cluster {}", clusterConfig.getId(), e);
                throw new HdfsLocalizedException("Ошибка Kerberos аутентификации: " + e.getMessage(),
                    org.springframework.http.HttpStatus.UNAUTHORIZED, "KerberosAuthException");
            }
        } else {
            hadoopConf.set("hadoop.security.authentication", "simple");
            this.loginUser = null;
        }
    }

    private URI configureFileSystemUri(ClusterConfig cluster, Configuration conf) {
        List<String> rpcUrls = cluster.getHdfsRpcUrls();
        if (rpcUrls != null && !rpcUrls.isEmpty()) {
            if (rpcUrls.size() == 1) {
                String url = rpcUrls.get(0);
                conf.set("fs.defaultFS", url);
                return URI.create(url);
            } else {
                // High Availability (HA) Failover конфигурация
                String nameservice = "ns-" + cluster.getId().replaceAll("[^a-zA-Z0-9]", "");
                conf.set("fs.defaultFS", "hdfs://" + nameservice);
                conf.set("dfs.nameservices", nameservice);

                List<String> nnIds = new ArrayList<>();
                for (int i = 0; i < rpcUrls.size(); i++) {
                    String nnId = "nn" + (i + 1);
                    nnIds.add(nnId);
                    String raw = rpcUrls.get(i).replace("hdfs://", "");
                    conf.set("dfs.namenode.rpc-address." + nameservice + "." + nnId, raw);
                }
                conf.set("dfs.ha.namenodes." + nameservice, String.join(",", nnIds));
                conf.set("dfs.client.failover.proxy.provider." + nameservice,
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
                return URI.create("hdfs://" + nameservice);
            }
        }

        // WebHDFS fallback
        List<String> webUrls = cluster.getWebhdfsUrls();
        if (webUrls != null && !webUrls.isEmpty()) {
            String url = webUrls.get(0);
            return URI.create(url);
        }

        return URI.create("hdfs://localhost:8020");
    }

    private <T> T executeAsUser(String username, PrivilegedExceptionAction<T> action) {
        String effectiveUser = (username != null && !username.isBlank()) ? username : "hdfs";
        try {
            UserGroupInformation ugi;
            if (loginUser != null) {
                // Kerberos Proxy User doAs имперсонация
                ugi = UserGroupInformation.createProxyUser(effectiveUser, loginUser);
            } else {
                ugi = UserGroupInformation.createRemoteUser(effectiveUser);
            }
            return ugi.doAs(action);
        } catch (Exception e) {
            Throwable cause = (e.getCause() != null) ? e.getCause() : e;
            var err = HdfsErrorTranslator.translate(cause);
            throw new HdfsLocalizedException(err.userFriendlyMessage(), err.status(), err.errorClass());
        }
    }

    @FunctionalInterface
    public interface PrivilegedFsAction<T> {
        T execute(FileSystem fs, Configuration conf) throws Exception;
    }

    public <T> T executeWithFileSystem(String doAsUser, PrivilegedFsAction<T> action) {
        return executeAsUser(doAsUser, () -> action.execute(getFileSystem(), hadoopConf));
    }

    private FileSystem getFileSystem() throws IOException {
        return FileSystem.get(fileSystemUri, hadoopConf);
    }

    @Override
    public List<HdfsFileStatus> listStatus(String path, String doAsUser) {
        return executeAsUser(doAsUser, () -> {
            FileSystem fs = getFileSystem();
            Path hdfsPath = new Path(path);
            FileStatus[] statuses = fs.listStatus(hdfsPath);
            List<HdfsFileStatus> result = new ArrayList<>();
            if (statuses != null) {
                for (FileStatus st : statuses) {
                    result.add(convert(st));
                }
            }
            return result;
        });
    }

    @Override
    public HdfsFileStatus getFileStatus(String path, String doAsUser) {
        return executeAsUser(doAsUser, () -> {
            FileSystem fs = getFileSystem();
            Path hdfsPath = new Path(path);
            FileStatus st = fs.getFileStatus(hdfsPath);
            return convert(st);
        });
    }

    @Override
    public InputStream open(String path, String doAsUser, long offset, Long length) {
        return executeAsUser(doAsUser, () -> {
            FileSystem fs = getFileSystem();
            Path hdfsPath = new Path(path);
            FSDataInputStream in = fs.open(hdfsPath);
            if (offset > 0) {
                in.seek(offset);
            }
            return in;
        });
    }

    @Override
    public void create(String path, InputStream data, String doAsUser, boolean overwrite) {
        executeAsUser(doAsUser, () -> {
            FileSystem fs = getFileSystem();
            Path hdfsPath = new Path(path);
            try (FSDataOutputStream out = fs.create(hdfsPath, overwrite)) {
                data.transferTo(out);
            }
            return null;
        });
    }

    @Override
    public boolean mkdirs(String path, String doAsUser) {
        return executeAsUser(doAsUser, () -> {
            FileSystem fs = getFileSystem();
            return fs.mkdirs(new Path(path));
        });
    }

    @Override
    public boolean rename(String src, String dst, String doAsUser) {
        return executeAsUser(doAsUser, () -> {
            FileSystem fs = getFileSystem();
            return fs.rename(new Path(src), new Path(dst));
        });
    }

    @Override
    public boolean delete(String path, String doAsUser, boolean recursive) {
        return executeAsUser(doAsUser, () -> {
            FileSystem fs = getFileSystem();
            return fs.delete(new Path(path), recursive);
        });
    }

    private HdfsFileStatus convert(FileStatus st) {
        return new HdfsFileStatus(
            st.getPath().getName(),
            st.isDirectory() ? "DIRECTORY" : "FILE",
            st.getLen(),
            st.getOwner(),
            st.getGroup(),
            st.getPermission().toString(),
            st.getAccessTime(),
            st.getModificationTime(),
            st.getBlockSize(),
            st.getReplication(),
            st.isDirectory() ? 0 : null
        );
    }
}
