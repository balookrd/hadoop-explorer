package org.apache.hadoop.explorer.replicator.hms.rewriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
     * Заданы ли оператором правила перезаписи локаций.
     *
     * <p>Если правила есть, путь мимо них — ошибка конфигурации: объект нельзя переносить с
     * локацией кластера-источника. Если правил нет, оператор выбрал режим сквозного переноса
     * (одинаковое пространство имен на обеих сторонах), и неперезаписанный путь ожидаем.
     */
    public boolean hasExplicitMappings() {
        return !federationMappings.isEmpty() || targetDefaultFs != null;
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
            // Разбор строкой, а не через java.net.URI: getPath() раскодирует %3A и прочие
            // экранирования метастора, и путь приемника расходился с реальным каталогом, а пробел
            // в значении партиции ронял URI.create — таблица падала целиком. Путь переносится как есть.
            String scheme = null;
            String authority = null;
            String path = sourceUri;
            int schemeEnd = sourceUri.indexOf("://");
            if (schemeEnd > 0) {
                scheme = sourceUri.substring(0, schemeEnd);
                String rest = sourceUri.substring(schemeEnd + 3);
                int slash = rest.indexOf('/');
                authority = slash < 0 ? rest : rest.substring(0, slash);
                path = slash < 0 ? "" : rest.substring(slash);
                if (authority.isEmpty()) {
                    authority = null;
                }
            }

            if (scheme == null || authority == null) {
                String targetPath = sourceUri;
                if (targetDefaultFs != null && !targetDefaultFs.isBlank()) {
                    String cleanTarget = targetDefaultFs.replaceAll("/+$", "");
                    String cleanPath = targetPath.startsWith("/") ? targetPath : "/" + targetPath;
                    return new PathRewriteResult(sourceUri, cleanTarget + cleanPath, defaultSourceCluster, defaultTargetCluster, false);
                }
                if (defaultSourceCluster != null && defaultTargetCluster != null && !defaultSourceCluster.isBlank()) {
                    String regex = "(?i)/" + Pattern.quote(defaultSourceCluster) + "(?=/|$)";
                    targetPath = targetPath.replaceAll(regex, "/" + Matcher.quoteReplacement(defaultTargetCluster));
                }
                return new PathRewriteResult(sourceUri, targetPath, defaultSourceCluster, defaultTargetCluster, false);
            }

            // 1. Проверка явных сопоставлений Federation Mappings
            for (FederationMapping mapping : federationMappings) {
                String srcNs = mapping.sourceNameservice();
                if (srcNs != null && (authority.equalsIgnoreCase(srcNs) || authority.toLowerCase().startsWith(srcNs.toLowerCase() + ":"))) {
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

            // 3. Динамическая замена по идентификаторам кластеров (по точным границам слов)
            String targetAuthority = authority;
            if (defaultSourceCluster != null && defaultTargetCluster != null && !defaultSourceCluster.isBlank()) {
                String wordBoundaryRegex = "(?i)\\b" + Pattern.quote(defaultSourceCluster) + "\\b";
                targetAuthority = authority.replaceAll(wordBoundaryRegex, Matcher.quoteReplacement(defaultTargetCluster));
            }

            String targetUri = scheme + "://" + targetAuthority + path;
            boolean unmapped = targetAuthority.equals(authority);
            if (unmapped) {
                log.debug("[HmsPathRewriter] Путь '{}' не имеет сопоставления федерации (targetDefaultFs не задан, authority '{}' не соответствует источнику '{}').",
                        sourceUri, authority, defaultSourceCluster);
            }

            return new PathRewriteResult(sourceUri, targetUri, defaultSourceCluster, defaultTargetCluster, unmapped);

        } catch (Exception e) {
            log.warn("[HmsPathRewriter] Ошибка парсинга URI '{}': {}. Использован оригинальный путь.", sourceUri, e.getMessage());
            return new PathRewriteResult(sourceUri, sourceUri, defaultSourceCluster, defaultTargetCluster, true);
        }
    }
}
