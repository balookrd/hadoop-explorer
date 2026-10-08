package org.apache.hadoop.explorer.yarn.model;

import java.util.ArrayList;
import java.util.List;

public class RoleMapping {
    private List<String> users = new ArrayList<>();
    private List<String> groups = new ArrayList<>();

    public RoleMapping() {}

    public RoleMapping(List<String> users, List<String> groups) {
        this.users = users != null ? users : new ArrayList<>();
        this.groups = groups != null ? groups : new ArrayList<>();
    }

    public List<String> getUsers() { return users; }
    public void setUsers(List<String> users) { this.users = users; }
    public List<String> getGroups() { return groups; }
    public void setGroups(List<String> groups) { this.groups = groups; }
}
