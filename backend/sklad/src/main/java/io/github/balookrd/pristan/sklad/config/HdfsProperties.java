package io.github.balookrd.pristan.sklad.config;

import io.github.balookrd.pristan.sklad.dto.cluster.ClusterConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "sklad")
public class HdfsProperties {

    private List<ClusterConfig> clusters = new ArrayList<>();
    private String clustersConfigPath;

    public List<ClusterConfig> getClusters() {
        return clusters;
    }

    public void setClusters(List<ClusterConfig> clusters) {
        this.clusters = clusters != null ? clusters : new ArrayList<>();
    }

    public String getClustersConfigPath() {
        return clustersConfigPath;
    }

    public void setClustersConfigPath(String clustersConfigPath) {
        this.clustersConfigPath = clustersConfigPath;
    }
}
