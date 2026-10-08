package org.apache.hadoop.explorer.yarn.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class YarnClientFactory {

    private final CommonSecurityProperties securityProperties;
    private final ObjectMapper objectMapper;

    public YarnClientFactory(CommonSecurityProperties securityProperties, ObjectMapper objectMapper) {
        this.securityProperties = securityProperties;
        this.objectMapper = objectMapper;
    }

    private final Map<String, YarnClient> clientCache = new ConcurrentHashMap<>();

    public YarnClient getClient(ClusterConfig cluster) {
        if ("mock".equalsIgnoreCase(securityProperties.getAuth().getMode())) {
            return new MockYarnClient(cluster);
        }

        return clientCache.computeIfAbsent(cluster.getId(), id -> new NativeYarnClient(cluster, objectMapper));
    }
}
