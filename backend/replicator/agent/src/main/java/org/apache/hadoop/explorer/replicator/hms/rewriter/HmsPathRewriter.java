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
    private final String targetDefaultFs;

    public HmsPathRewriter() {
        this(Collections.emptyList(), null);
    }

    public HmsPathRewriter(List<FederationMapping> mappings) {
        this(mappings, null);
    }

    public HmsPathRewriter(List<FederationMapping> mappings, String targetDefaultFs) {
        this.federationMappings = mappings != null ? new ArrayList<>(mappings) : Collections.emptyList();
        this.targetDefaultFs = (targetDefaultFs != null && !targetDefaultFs.isBlank()) ? targetDefaultFs.trim() : null;
    }

    /**
     * Парсинг строки сопоставления федеративных nameservices или хостов.
     * Формат: "src1=dst1,src2=dst2" или "src1->dst1;src2->dst2".
     */
    public static List<FederationMapping> parseMappings(String mappingsStr) {
        if (mappingsStr == null || mappingsStr.isBlank()) {
            return Collections.emptyList();
        }
        List<FederationMapping> list = new ArrayList<>();
        String[] pairs = mappingsStr.split("[,;]");
        for (String pair : pairs) {
            String p = pair.trim();
            if (p.isEmpty()) continue;
            String[] parts = p.contains("->") ? p.split("->", 2) : p.split("=", 2);
            if (parts.length == 2 && !parts[0].isBlank() && !parts[1].isBlank()) {
                list.add(new FederationMapping(parts[0].trim(), parts[1].trim()));
            }
        }
        return list;
    }

    /**
     * Трансляция пути HDFS с учетом федерации (HDFS Federation / ViewFS) и межкластерных сопоставлений.
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
                if (targetDefaultFs != null && !targetDefaultFs.isBlank()) {
                    String cleanTarget = targetDefaultFs.replaceAll("/+$", "");
                    String cleanPath = targetPath.startsWith("/") ? targetPath : "/" + targetPath;
                    return new PathRewriteResult(sourceUri, cleanTarget + cleanPath, defaultSourceCluster, defaultTargetCluster, false);
                }
                if (defaultSourceCluster != null && defaultTargetCluster != null) {
                    targetPath = targetPath.replace("/" + defaultSourceCluster + "/", "/" + defaultTargetCluster + "/")
                                           .replace("/" + defaultSourceCluster, "/" + defaultTargetCluster);
                }
                return new PathRewriteResult(sourceUri, targetPath, defaultSourceCluster, defaultTargetCluster, false);
            }

            // 1. Проверка явных Federation / Cluster Mappings
            for (FederationMapping mapping : federationMappings) {
                String srcNs = mapping.sourceNameservice();
                if (srcNs != null && (authority.equalsIgnoreCase(srcNs) || authority.startsWith(srcNs + ":") || authority.contains(srcNs))) {
                    String targetNs = mapping.targetNameservice();
                    if (targetNs.contains("://")) {
                        String targetPrefix = targetNs.endsWith("/") ? targetNs.substring(0, targetNs.length() - 1) : targetNs;
                        String targetUri = targetPrefix + path;
                        return new PathRewriteResult(sourceUri, targetUri, defaultSourceCluster, defaultTargetCluster, false);
                    } else {
                        String targetAuthority = targetNs;
                        String targetUri = scheme + "://" + targetAuthority + path;
                        return new PathRewriteResult(sourceUri, targetUri, defaultSourceCluster, defaultTargetCluster, false);
                    }
                }
            }

            // 2. Если задан targetDefaultFs, транслируем authority на целевой FS
            if (targetDefaultFs != null && !targetDefaultFs.isBlank()) {
                URI targetFsUri = URI.create(targetDefaultFs);
                String targetFsScheme = targetFsUri.getScheme() != null ? targetFsUri.getScheme() : scheme;
                String targetFsAuthority = targetFsUri.getAuthority();
                if (targetFsAuthority != null) {
                    String targetUri = targetFsScheme + "://" + targetFsAuthority + path;
                    return new PathRewriteResult(sourceUri, targetUri, defaultSourceCluster, defaultTargetCluster, false);
                }
            }

            // 3. Fallback: замена хоста/NS источника на целевой с учетом распространенных паттернов кластеров
            String targetAuthority = authority;
            if (defaultSourceCluster != null && defaultTargetCluster != null && authority.contains(defaultSourceCluster)) {
                targetAuthority = authority.replace(defaultSourceCluster, defaultTargetCluster);
            }

            if (targetAuthority.equals(authority)) {
                targetAuthority = translateClusterAuthority(authority, defaultSourceCluster, defaultTargetCluster);
            }

            String targetUri = scheme + "://" + targetAuthority + path;
            boolean unmapped = targetAuthority.equals(authority);
            return new PathRewriteResult(sourceUri, targetUri, defaultSourceCluster, defaultTargetCluster, unmapped);

        } catch (Exception e) {
            log.warn("Ошибка парсинга URI '{}': {}. Использован оригинальный путь.", sourceUri, e.getMessage());
            return new PathRewriteResult(sourceUri, sourceUri, defaultSourceCluster, defaultTargetCluster, true);
        }
    }

    public static String translateClusterAuthority(String authority, String srcCluster, String dstCluster) {
        if (authority == null) return null;
        String res = authority;

        boolean srcHas1 = (srcCluster != null && srcCluster.contains("1")) || authority.contains("1");
        boolean dstHas2 = (dstCluster != null && dstCluster.contains("2"));
        boolean srcHas2 = (srcCluster != null && srcCluster.contains("2")) || authority.contains("2");
        boolean dstHas1 = (dstCluster != null && dstCluster.contains("1"));

        if (srcHas1 && dstHas2) {
            res = res.replace("hdfs-cluster-1", "hdfs-cluster-2")
                     .replace("cluster-1", "cluster-2")
                     .replace("cluster1", "cluster2")
                     .replace("namenode-dc1", "namenode-dc2")
                     .replace("dc1", "dc2")
                     .replace("dc-1", "dc-2");
        } else if (srcHas2 && dstHas1) {
            res = res.replace("hdfs-cluster-2", "hdfs-cluster-1")
                     .replace("cluster-2", "cluster-1")
                     .replace("cluster2", "cluster1")
                     .replace("namenode-dc2", "namenode-dc1")
                     .replace("dc2", "dc1")
                     .replace("dc-2", "dc-1");
        }

        return res;
    }
}
