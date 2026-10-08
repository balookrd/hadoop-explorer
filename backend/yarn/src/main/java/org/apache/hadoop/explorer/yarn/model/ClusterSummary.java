package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.explorer.common.model.Role;

import java.util.ArrayList;
import java.util.List;

public class ClusterSummary {
    private String id;
    private String name;
    private String description = "";

    @JsonProperty("active_rm_url")
    private String activeRmUrl;

    @JsonProperty("kerberos_enabled")
    private boolean kerberosEnabled;

    @JsonProperty("impersonation_enabled")
    private boolean impersonationEnabled;

    private List<String> partitions = new ArrayList<>();

    @JsonProperty("default_partition")
    private String defaultPartition;

    @JsonProperty("resource_mode")
    private String resourceMode;

    @JsonProperty("total_resources")
    private ClusterResources totalResources;

    @JsonProperty("user_role")
    private Role userRole;

    @JsonProperty("can_write")
    private boolean canWrite;

    @JsonProperty("can_admin")
    private boolean canAdmin;

    public ClusterSummary() {}

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private final ClusterSummary obj = new ClusterSummary();

        public Builder id(String id) { obj.id = id; return this; }
        public Builder name(String name) { obj.name = name; return this; }
        public Builder description(String description) { obj.description = description; return this; }
        public Builder activeRmUrl(String activeRmUrl) { obj.activeRmUrl = activeRmUrl; return this; }
        public Builder kerberosEnabled(boolean kerberosEnabled) { obj.kerberosEnabled = kerberosEnabled; return this; }
        public Builder impersonationEnabled(boolean impersonationEnabled) { obj.impersonationEnabled = impersonationEnabled; return this; }
        public Builder partitions(List<String> partitions) { obj.partitions = partitions; return this; }
        public Builder defaultPartition(String defaultPartition) { obj.defaultPartition = defaultPartition; return this; }
        public Builder resourceMode(String resourceMode) { obj.resourceMode = resourceMode; return this; }
        public Builder totalResources(ClusterResources totalResources) { obj.totalResources = totalResources; return this; }
        public Builder userRole(Role userRole) { obj.userRole = userRole; return this; }
        public Builder canWrite(boolean canWrite) { obj.canWrite = canWrite; return this; }
        public Builder canAdmin(boolean canAdmin) { obj.canAdmin = canAdmin; return this; }

        public ClusterSummary build() { return obj; }
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getActiveRmUrl() { return activeRmUrl; }
    public void setActiveRmUrl(String activeRmUrl) { this.activeRmUrl = activeRmUrl; }
    public boolean isKerberosEnabled() { return kerberosEnabled; }
    public void setKerberosEnabled(boolean kerberosEnabled) { this.kerberosEnabled = kerberosEnabled; }
    public boolean isImpersonationEnabled() { return impersonationEnabled; }
    public void setImpersonationEnabled(boolean impersonationEnabled) { this.impersonationEnabled = impersonationEnabled; }
    public List<String> getPartitions() { return partitions; }
    public void setPartitions(List<String> partitions) { this.partitions = partitions; }
    public String getDefaultPartition() { return defaultPartition; }
    public void setDefaultPartition(String defaultPartition) { this.defaultPartition = defaultPartition; }
    public String getResourceMode() { return resourceMode; }
    public void setResourceMode(String resourceMode) { this.resourceMode = resourceMode; }
    public ClusterResources getTotalResources() { return totalResources; }
    public void setTotalResources(ClusterResources totalResources) { this.totalResources = totalResources; }
    public Role getUserRole() { return userRole; }
    public void setUserRole(Role userRole) { this.userRole = userRole; }
    public boolean isCanWrite() { return canWrite; }
    public void setCanWrite(boolean canWrite) { this.canWrite = canWrite; }
    public boolean isCanAdmin() { return canAdmin; }
    public void setCanAdmin(boolean canAdmin) { this.canAdmin = canAdmin; }
}
