package org.apache.hadoop.explorer.replicator.hms.rewriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HmsPathRewriter {

    private static final Logger log = LoggerFactory.getLogger(HmsPathRewriter.class);

    public record FederationMapping(String sourceNameservice, String targetNameservice) {}

    public record PathRewriteResult(
            String sourceUri,
            String targetUri,
            String sourceClusterId,
            String targetClusterId,
            boolean unmapped
    ) {}

    private final List<FederationMapping> federationMappings;

    public HmsPathRewriter() {
        this(Collections.emptyList());
    }

    public HmsPathRewriter(List<FederationMapping> mappings) {
        this.federationMappings = mappings != null ? new ArrayList<>(mappings) : Collections.emptyList();
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
            String authority = uri.getAuthority();
            String path = uri.getPath();

            if (scheme == null || authority == null) {
                String targetPath = sourceUri;
                if (defaultSourceCluster != null && defaultTargetCluster != null) {
                    targetPath = targetPath.replace("/" + defaultSourceCluster + "/", "/" + defaultTargetCluster + "/")
                                           .replace("/" + defaultSourceCluster, "/" + defaultTargetCluster);
                }
                return new PathRewriteResult(sourceUri, targetPath, defaultSourceCluster, defaultTargetCluster, false);
            }

            for (FederationMapping mapping : federationMappings) {
                String srcNs = mapping.sourceNameservice();
                if (srcNs != null && (authority.equalsIgnoreCase(srcNs) || authority.startsWith(srcNs + ":") || authority.contains(srcNs))) {
                    String targetNs = mapping.targetNameservice();
                    String targetPrefix = targetNs.endsWith("/") ? targetNs.substring(0, targetNs.length() - 1) : targetNs;
                    String targetUri = targetPrefix + path;
                    return new PathRewriteResult(sourceUri, targetUri, defaultSourceCluster, defaultTargetCluster, false);
                }
            }

            // Fallback: замена хоста/NS источника на целевой
            String targetAuthority = authority;
            if (defaultSourceCluster != null && defaultTargetCluster != null) {
                targetAuthority = authority.replace(defaultSourceCluster, defaultTargetCluster);
            }
            String targetUri = scheme + "://" + targetAuthority + path;
            return new PathRewriteResult(sourceUri, targetUri, defaultSourceCluster, defaultTargetCluster, true);

        } catch (Exception e) {
            log.warn("Ошибка парсинга URI '{}': {}. Использован оригинальный путь.", sourceUri, e.getMessage());
            return new PathRewriteResult(sourceUri, sourceUri, defaultSourceCluster, defaultTargetCluster, true);
        }
    }
}
