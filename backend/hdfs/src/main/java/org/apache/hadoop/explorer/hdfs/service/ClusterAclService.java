package org.apache.hadoop.explorer.hdfs.service;

import org.apache.hadoop.explorer.hdfs.dto.cluster.ClusterAcl;
import org.apache.hadoop.explorer.hdfs.dto.cluster.ClusterConfig;
import org.apache.hadoop.explorer.hdfs.dto.cluster.ClusterPublicInfo;
import org.apache.hadoop.explorer.hdfs.exception.HdfsLocalizedException;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class ClusterAclService {

    private final CommonSecurityProperties securityProperties;

    public ClusterAclService(CommonSecurityProperties securityProperties) {
        this.securityProperties = securityProperties;
    }

    public boolean isGlobalAdmin(String username, Collection<String> userGroups) {
        if (securityProperties.getAuth() == null || securityProperties.getAuth().getAdminGroups() == null) {
            return false;
        }
        Set<String> adminGroups = new HashSet<>();
        for (String g : securityProperties.getAuth().getAdminGroups()) {
            adminGroups.add(g.toLowerCase());
        }
        for (String g : userGroups) {
            if (adminGroups.contains(g.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    public boolean canAccessCluster(ClusterConfig cluster, String username, Collection<String> userGroups) {
        if (isGlobalAdmin(username, userGroups)) {
            return true;
        }

        ClusterAcl acl = cluster.getAcl();
        if (acl == null) {
            return true;
        }

        if (acl.getAllowedGroups().isEmpty() && acl.getAllowedUsers().isEmpty()) {
            return true;
        }

        String uLower = username != null ? username.toLowerCase() : "";
        for (String u : acl.getAllowedUsers()) {
            if (u.equalsIgnoreCase(uLower)) {
                return true;
            }
        }

        Set<String> groupsLower = new HashSet<>();
        for (String g : userGroups) {
            groupsLower.add(g.toLowerCase());
        }

        for (String g : acl.getAllowedGroups()) {
            if (groupsLower.contains(g.toLowerCase())) {
                return true;
            }
        }

        for (String g : acl.getAdminGroups()) {
            if (groupsLower.contains(g.toLowerCase())) {
                return true;
            }
        }

        return false;
    }

    public boolean isClusterReadOnly(ClusterConfig cluster, String username, Collection<String> userGroups) {
        if (isGlobalAdmin(username, userGroups)) {
            return false;
        }

        ClusterAcl acl = cluster.getAcl();
        if (acl == null) {
            return false;
        }

        Set<String> groupsLower = new HashSet<>();
        for (String g : userGroups) {
            groupsLower.add(g.toLowerCase());
        }

        for (String g : acl.getAdminGroups()) {
            if (groupsLower.contains(g.toLowerCase())) {
                return false;
            }
        }

        for (String g : acl.getReadOnlyGroups()) {
            if (groupsLower.contains(g.toLowerCase())) {
                return true;
            }
        }

        return false;
    }

    public boolean isClusterAdmin(ClusterConfig cluster, String username, Collection<String> userGroups) {
        if (isGlobalAdmin(username, userGroups)) {
            return true;
        }
        ClusterAcl acl = cluster.getAcl();
        if (acl == null) {
            return false;
        }
        Set<String> groupsLower = new HashSet<>();
        for (String g : userGroups) {
            groupsLower.add(g.toLowerCase());
        }
        for (String g : acl.getAdminGroups()) {
            if (groupsLower.contains(g.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    public List<ClusterPublicInfo> getVisibleClusters(List<ClusterConfig> allClusters, String username, Collection<String> userGroups) {
        List<ClusterPublicInfo> result = new ArrayList<>();
        for (ClusterConfig c : allClusters) {
            if (canAccessCluster(c, username, userGroups)) {
                String defaultPath = c.getDefaultPath().replace("{username}", username != null ? username : "");
                result.add(new ClusterPublicInfo(
                    c.getId(),
                    c.getName(),
                    c.getDescription(),
                    defaultPath,
                    isClusterReadOnly(c, username, userGroups),
                    isClusterAdmin(c, username, userGroups)
                ));
            }
        }
        return result;
    }

    public void validateWriteAccess(ClusterConfig cluster, String username, Collection<String> userGroups) {
        if (isClusterReadOnly(cluster, username, userGroups)) {
            throw new HdfsLocalizedException(
                "Кластер '" + cluster.getId() + "' доступен только для чтения (Read-Only) для вашей учетной записи/группы",
                HttpStatus.FORBIDDEN,
                "ClusterReadOnlyException"
            );
        }
    }
}
