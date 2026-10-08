package org.apache.hadoop.explorer.yarn.model;

public class RolesConfig {
    private RoleMapping admin = new RoleMapping();
    private RoleMapping writer = new RoleMapping();
    private RoleMapping reader = new RoleMapping();

    public RolesConfig() {}

    public RoleMapping getAdmin() { return admin; }
    public void setAdmin(RoleMapping admin) { this.admin = admin; }
    public RoleMapping getWriter() { return writer; }
    public void setWriter(RoleMapping writer) { this.writer = writer; }
    public RoleMapping getReader() { return reader; }
    public void setReader(RoleMapping reader) { this.reader = reader; }
}
