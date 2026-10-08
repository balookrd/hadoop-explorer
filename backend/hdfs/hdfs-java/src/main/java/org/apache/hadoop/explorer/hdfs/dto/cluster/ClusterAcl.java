package org.apache.hadoop.explorer.hdfs.dto.cluster;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

public class ClusterAcl {

    @JsonProperty("allowed_groups")
    private List<String> allowedGroups = new ArrayList<>();

    @JsonProperty("allowed_users")
    private List<String> allowedUsers = new ArrayList<>();

    @JsonProperty("read_only_groups")
    private List<String> readOnlyGroups = new ArrayList<>();

    @JsonProperty("admin_groups")
    private List<String> adminGroups = new ArrayList<>();

    public ClusterAcl() {}

    public List<String> getAllowedGroups() { return allowedGroups; }
    public void setAllowedGroups(List<String> allowedGroups) { this.allowedGroups = allowedGroups != null ? allowedGroups : new ArrayList<>(); }

    public List<String> getAllowedUsers() { return allowedUsers; }
    public void setAllowedUsers(List<String> allowedUsers) { this.allowedUsers = allowedUsers != null ? allowedUsers : new ArrayList<>(); }

    public List<String> getReadOnlyGroups() { return readOnlyGroups; }
    public void setReadOnlyGroups(List<String> readOnlyGroups) { this.readOnlyGroups = readOnlyGroups != null ? readOnlyGroups : new ArrayList<>(); }

    public List<String> getAdminGroups() { return adminGroups; }
    public void setAdminGroups(List<String> adminGroups) { this.adminGroups = adminGroups != null ? adminGroups : new ArrayList<>(); }
}
