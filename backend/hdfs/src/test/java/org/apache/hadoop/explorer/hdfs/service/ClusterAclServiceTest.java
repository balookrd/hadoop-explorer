package org.apache.hadoop.explorer.hdfs.service;

import org.apache.hadoop.explorer.hdfs.dto.cluster.ClusterAcl;
import org.apache.hadoop.explorer.hdfs.dto.cluster.ClusterConfig;
import org.apache.hadoop.explorer.hdfs.dto.cluster.ClusterPublicInfo;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ClusterAclServiceTest {

    private ClusterAclService aclService;
    private ClusterConfig cluster;

    @BeforeEach
    void setUp() {
        CommonSecurityProperties props = new CommonSecurityProperties();
        props.getAuth().setAdminGroups(Set.of("hadoop-admins", "superusers"));

        aclService = new ClusterAclService(props);

        cluster = new ClusterConfig();
        cluster.setId("test-cluster");
        cluster.setName("Test Cluster");

        ClusterAcl acl = new ClusterAcl();
        acl.setAllowedGroups(List.of("developers", "analysts"));
        acl.setReadOnlyGroups(List.of("analysts"));
        acl.setAdminGroups(List.of("lead-devs"));
        cluster.setAcl(acl);
    }

    @Test
    void testGlobalAdminBypass() {
        assertTrue(aclService.isGlobalAdmin("root", List.of("hadoop-admins")));
        assertTrue(aclService.canAccessCluster(cluster, "root", List.of("hadoop-admins")));
        assertFalse(aclService.isClusterReadOnly(cluster, "root", List.of("hadoop-admins")));
        assertTrue(aclService.isClusterAdmin(cluster, "root", List.of("hadoop-admins")));
    }

    @Test
    void testDeveloperWriteAccess() {
        assertTrue(aclService.canAccessCluster(cluster, "john", List.of("developers")));
        assertFalse(aclService.isClusterReadOnly(cluster, "john", List.of("developers")));
        assertFalse(aclService.isClusterAdmin(cluster, "john", List.of("developers")));
    }

    @Test
    void testAnalystReadOnlyAccess() {
        assertTrue(aclService.canAccessCluster(cluster, "anna", List.of("analysts")));
        assertTrue(aclService.isClusterReadOnly(cluster, "anna", List.of("analysts")));
    }

    @Test
    void testForbiddenGroup() {
        assertFalse(aclService.canAccessCluster(cluster, "guest", List.of("guests")));
    }

    @Test
    void testGetVisibleClusters() {
        List<ClusterPublicInfo> visible = aclService.getVisibleClusters(
            List.of(cluster), "anna", List.of("analysts"));
        assertEquals(1, visible.size());
        assertTrue(visible.get(0).isReadOnly());
    }
}
