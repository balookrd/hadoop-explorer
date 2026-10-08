package org.apache.hadoop.explorer.yarn.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class ClusterAcl {
    @JsonProperty("allowed_users")
    private List<String> allowedUsers = new ArrayList<>(List.of("*"));

    @JsonProperty("allowed_groups")
    private List<String> allowedGroups = new ArrayList<>(List.of("*"));

    private RolesConfig roles = new RolesConfig();

    public ClusterAcl() {}

    public List<String> getAllowedUsers() { return allowedUsers; }
    public void setAllowedUsers(List<String> allowedUsers) { this.allowedUsers = allowedUsers; }
    public List<String> getAllowedGroups() { return allowedGroups; }
    public void setAllowedGroups(List<String> allowedGroups) { this.allowedGroups = allowedGroups; }
    public RolesConfig getRoles() { return roles; }
    public void setRoles(RolesConfig roles) { this.roles = roles; }
}
