package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class ClusterConfig {
    private String id;
    private String name;
    private String description = "";

    @JsonProperty("resource_manager_urls")
    private List<String> resourceManagerUrls = new ArrayList<>();

    @JsonProperty("kerberos_enabled")
    private boolean kerberosEnabled = false;

    @JsonProperty("kerberos_principal")
    private String kerberosPrincipal;

    @JsonProperty("impersonation_enabled")
    private boolean impersonationEnabled = true;

    @JsonProperty("default_partition")
    private String defaultPartition = "DEFAULT";

    private List<String> partitions = new ArrayList<>(List.of("DEFAULT"));

    @JsonProperty("resource_mode")
    private String resourceMode = "percentage";

    @JsonProperty("queue_mappings")
    private String queueMappings = "u:%user:%user,g:hadoop-admins:root.production";

    @JsonProperty("queue_mappings_override")
    private boolean queueMappingsOverride = false;

    @JsonProperty("capacity_scheduler_xml_path")
    private String capacitySchedulerXmlPath;

    @JsonProperty("total_resources")
    private ClusterResources totalResources;

    private ClusterAcl acl = new ClusterAcl();
    private AwxClusterConfig awx = new AwxClusterConfig();

    public ClusterConfig() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final ClusterConfig obj = new ClusterConfig();

        public Builder id(String id) { obj.id = id; return this; }
        public Builder name(String name) { obj.name = name; return this; }
        public Builder description(String description) { obj.description = description; return this; }
        public Builder resourceManagerUrls(List<String> urls) { obj.resourceManagerUrls = urls; return this; }
        public Builder kerberosEnabled(boolean kerberosEnabled) { obj.kerberosEnabled = kerberosEnabled; return this; }
        public Builder kerberosPrincipal(String principal) { obj.kerberosPrincipal = principal; return this; }
        public Builder impersonationEnabled(boolean impersonationEnabled) { obj.impersonationEnabled = impersonationEnabled; return this; }
        public Builder defaultPartition(String defaultPartition) { obj.defaultPartition = defaultPartition; return this; }
        public Builder partitions(List<String> partitions) { obj.partitions = partitions; return this; }
        public Builder resourceMode(String resourceMode) { obj.resourceMode = resourceMode; return this; }
        public Builder queueMappings(String queueMappings) { obj.queueMappings = queueMappings; return this; }
        public Builder queueMappingsOverride(boolean override) { obj.queueMappingsOverride = override; return this; }
        public Builder capacitySchedulerXmlPath(String path) { obj.capacitySchedulerXmlPath = path; return this; }
        public Builder totalResources(ClusterResources totalResources) { obj.totalResources = totalResources; return this; }
        public Builder acl(ClusterAcl acl) { obj.acl = acl; return this; }
        public Builder awx(AwxClusterConfig awx) { obj.awx = awx; return this; }

        public ClusterConfig build() { return obj; }
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public List<String> getResourceManagerUrls() { return resourceManagerUrls; }
    public void setResourceManagerUrls(List<String> resourceManagerUrls) { this.resourceManagerUrls = resourceManagerUrls; }
    public boolean isKerberosEnabled() { return kerberosEnabled; }
    public void setKerberosEnabled(boolean kerberosEnabled) { this.kerberosEnabled = kerberosEnabled; }
    public String getKerberosPrincipal() { return kerberosPrincipal; }
    public void setKerberosPrincipal(String kerberosPrincipal) { this.kerberosPrincipal = kerberosPrincipal; }
    public boolean isImpersonationEnabled() { return impersonationEnabled; }
    public void setImpersonationEnabled(boolean impersonationEnabled) { this.impersonationEnabled = impersonationEnabled; }
    public String getDefaultPartition() { return defaultPartition; }
    public void setDefaultPartition(String defaultPartition) { this.defaultPartition = defaultPartition; }
    public List<String> getPartitions() { return partitions; }
    public void setPartitions(List<String> partitions) { this.partitions = partitions; }
    public String getResourceMode() { return resourceMode; }
    public void setResourceMode(String resourceMode) { this.resourceMode = resourceMode; }
    public String getQueueMappings() { return queueMappings; }
    public void setQueueMappings(String queueMappings) { this.queueMappings = queueMappings; }
    public boolean isQueueMappingsOverride() { return queueMappingsOverride; }
    public void setQueueMappingsOverride(boolean queueMappingsOverride) { this.queueMappingsOverride = queueMappingsOverride; }
    public String getCapacitySchedulerXmlPath() { return capacitySchedulerXmlPath; }
    public void setCapacitySchedulerXmlPath(String capacitySchedulerXmlPath) { this.capacitySchedulerXmlPath = capacitySchedulerXmlPath; }
    public ClusterResources getTotalResources() { return totalResources; }
    public void setTotalResources(ClusterResources totalResources) { this.totalResources = totalResources; }
    public ClusterAcl getAcl() { return acl; }
    public void setAcl(ClusterAcl acl) { this.acl = acl; }
    public AwxClusterConfig getAwx() { return awx; }
    public void setAwx(AwxClusterConfig awx) { this.awx = awx; }
}
