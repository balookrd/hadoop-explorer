package org.apache.hadoop.explorer.replicator.orchestrator.hms.rewriter;

import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.List;

@Component
public class HmsPathRewriter {

    private static final Logger log = LoggerFactory.getLogger(HmsPathRewriter.class);

    private final ReplicatorProperties properties;

    public record PathRewriteResult(
            String sourceUri,
            String targetUri,
            String sourceClusterId,
            String targetClusterId,
            boolean unmapped
    ) {}

    public HmsPathRewriter(ReplicatorProperties properties) {
        this.properties = properties;
    }

    /**
     * Трансляция пути HDFS с учетом федерации (HDFS Federation / ViewFS).
     */
    public PathRewriteResult rewrite(String sourceUri, String defaultSourceCluster, String defaultTargetCluster) {
        if (sourceUri == null || sourceUri.isBlank()) {
            return new PathRewriteResult(sourceUri, sourceUri, defaultSourceCluster, defaultTargetCluster, false);
        }

        try {
            URI uri = URI.create(sourceUri);
            String scheme = uri.getScheme();
            String authority = uri.getAuthority(); // например: ns-cold, ns-cold:8020, namenode-dc1:8020
            String path = uri.getPath();

            if (scheme == null || authority == null) {
                // Локальный путь тома (например /tmp/data/dc1/...): транслируем в целевой кластер dc2
                String targetPath = sourceUri;
                if (defaultSourceCluster != null && defaultTargetCluster != null) {
                    targetPath = targetPath.replace("/" + defaultSourceCluster + "/", "/" + defaultTargetCluster + "/")
                                           .replace("/" + defaultSourceCluster, "/" + defaultTargetCluster);
                }
                return new PathRewriteResult(sourceUri, targetPath, defaultSourceCluster, defaultTargetCluster, false);
            }

            List<ReplicatorProperties.FederationMappingConfig> mappings = properties.getFederationMappings();
            if (mappings != null) {
                for (var mapping : mappings) {
                    String srcNs = mapping.getSourceNameservice();
                    if (srcNs != null && (authority.equalsIgnoreCase(srcNs) || authority.startsWith(srcNs + ":") || authority.contains(srcNs))) {
                        String targetNs = mapping.getTargetNameservice(); // например: hdfs://ns-cold-dc2:8020
                        String targetPrefix = targetNs.endsWith("/") ? targetNs.substring(0, targetNs.length() - 1) : targetNs;
                        String targetPath = path.startsWith("/") ? path : "/" + path;
                        String rewrittenUri = targetPrefix + targetPath;

                        String srcCl = mapping.getSourceClusterId() != null ? mapping.getSourceClusterId() : defaultSourceCluster;
                        String dstCl = mapping.getTargetClusterId() != null ? mapping.getTargetClusterId() : defaultTargetCluster;

                        log.debug("Федеративный маппинг: '{}' -> '{}' (кластеры: {} -> {})", sourceUri, rewrittenUri, srcCl, dstCl);
                        return new PathRewriteResult(sourceUri, rewrittenUri, srcCl, dstCl, false);
                    }
                }
            }

            // Fallback, если неймсервис не описан явно в federationMappings:
            // Пытаемся заменить префикс кластера dc1 на dc2 в имени хоста
            String fallbackTargetUri = sourceUri.replace("dc1", "dc2");
            log.warn("Не найдено правило федерации для authority '{}' в URI '{}'. Использован fallback: '{}'",
                    authority, sourceUri, fallbackTargetUri);

            return new PathRewriteResult(sourceUri, fallbackTargetUri, defaultSourceCluster, defaultTargetCluster, true);

        } catch (Exception e) {
            log.error("Ошибка парсинга URI '{}': {}", sourceUri, e.getMessage());
            return new PathRewriteResult(sourceUri, sourceUri, defaultSourceCluster, defaultTargetCluster, true);
        }
    }
}
