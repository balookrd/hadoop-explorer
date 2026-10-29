package org.apache.hadoop.explorer.yarn.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.explorer.common.config.CommonSecurityProperties;
import org.apache.hadoop.explorer.yarn.config.YarnProperties;
import org.apache.hadoop.explorer.yarn.model.ClusterConfig;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class YarnClientFactory {

    private final CommonSecurityProperties securityProperties;
    private final YarnProperties yarnProperties;
    private final ObjectMapper objectMapper;
    private final KerberosClientLogin kerberosLogin;

    public YarnClientFactory(CommonSecurityProperties securityProperties,
                             YarnProperties yarnProperties,
                             ObjectMapper objectMapper,
                             KerberosClientLogin kerberosLogin) {
        this.securityProperties = securityProperties;
        this.yarnProperties = yarnProperties;
        this.objectMapper = objectMapper;
        this.kerberosLogin = kerberosLogin;
    }

    private final Map<String, YarnClient> clientCache = new ConcurrentHashMap<>();

    public YarnClient getClient(ClusterConfig cluster) {
        // Driven by yarn.client-mode, not by the authentication mode: a misconfigured login
        // mode must never switch a production cluster to fake queue data
        if (yarnProperties.isMockClient()) {
            return new MockYarnClient(cluster);
        }

        return clientCache.computeIfAbsent(cluster.getId(), id -> new NativeYarnClient(
                cluster,
                objectMapper,
                securityProperties != null ? securityProperties.getTls() : null,
                kerberosLogin
        ));
    }
}
