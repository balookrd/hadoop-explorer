package org.apache.hadoop.explorer.hdfs.dto.cluster;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

public class ClusterConfig {

    private String id;
    private String name;
    private String description = "";

    @JsonProperty("webhdfs_urls")
    private List<String> webhdfsUrls = new ArrayList<>();

    @JsonProperty("hdfs_rpc_urls")
    private List<String> hdfsRpcUrls = new ArrayList<>();

    @JsonProperty("auth_type")
    private String authType = "simple"; // "simple" or "kerberos"

    @JsonProperty("service_principal")
    private String servicePrincipal;

    @JsonProperty("namenode_principal")
    private String namenodePrincipal;

    @JsonProperty("keytab_path")
    private String keytabPath;

    @JsonProperty("timeout_seconds")
    private int timeoutSeconds = 30;

    @JsonProperty("preview_max_bytes")
    private int previewMaxBytes = 1048576; // 1 MB

    @JsonProperty("default_path")
    private String defaultPath = "/user/{username}";

    @JsonProperty("mock_storage")
    private boolean mockStorage = false;

    private ClusterAcl acl = new ClusterAcl();

    public ClusterConfig() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description != null ? description : ""; }

    public List<String> getWebhdfsUrls() { return webhdfsUrls; }
    public void setWebhdfsUrls(List<String> webhdfsUrls) { this.webhdfsUrls = webhdfsUrls != null ? webhdfsUrls : new ArrayList<>(); }

    public List<String> getHdfsRpcUrls() { return hdfsRpcUrls; }
    public void setHdfsRpcUrls(List<String> hdfsRpcUrls) { this.hdfsRpcUrls = hdfsRpcUrls != null ? hdfsRpcUrls : new ArrayList<>(); }

    public String getAuthType() { return authType; }
    public void setAuthType(String authType) { this.authType = authType; }

    public String getServicePrincipal() { return servicePrincipal; }
    public void setServicePrincipal(String servicePrincipal) { this.servicePrincipal = servicePrincipal; }

    public String getNamenodePrincipal() { return namenodePrincipal; }
    public void setNamenodePrincipal(String namenodePrincipal) { this.namenodePrincipal = namenodePrincipal; }

    public String getKeytabPath() { return keytabPath; }
    public void setKeytabPath(String keytabPath) { this.keytabPath = keytabPath; }

    public int getTimeoutSeconds() { return timeoutSeconds; }
    public void setTimeoutSeconds(int timeoutSeconds) { this.timeoutSeconds = timeoutSeconds; }

    public int getPreviewMaxBytes() { return previewMaxBytes; }
    public void setPreviewMaxBytes(int previewMaxBytes) { this.previewMaxBytes = previewMaxBytes; }

    public String getDefaultPath() { return defaultPath; }
    public void setDefaultPath(String defaultPath) { this.defaultPath = defaultPath; }

    public boolean isMockStorage() { return mockStorage; }
    public void setMockStorage(boolean mockStorage) { this.mockStorage = mockStorage; }

    public ClusterAcl getAcl() { return acl; }
    public void setAcl(ClusterAcl acl) { this.acl = acl != null ? acl : new ClusterAcl(); }
}
