package org.apache.hadoop.explorer.replicator.orchestrator.topology;

import org.apache.hadoop.explorer.replicator.model.ClusterDto;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;
import org.apache.hadoop.explorer.replicator.orchestrator.dto.TopologyResponse;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class TopologyRegistry {

    private final List<ReplicatorProperties.DatacenterConfig> datacenters = new ArrayList<>();
    private final Map<String, ReplicatorProperties.ClusterConfig> clusters = new ConcurrentHashMap<>();
    private final Map<String, Long> dcLimits = new ConcurrentHashMap<>();
    /** Каналы, лимит которых задан явно, а не подставлен по умолчанию. */
    private final Set<String> explicitDcLimits = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> hdfsLimits = new ConcurrentHashMap<>();

    public TopologyRegistry(ReplicatorProperties properties) {
        if (properties.getDatacenters() != null) {
            this.datacenters.addAll(properties.getDatacenters());
        }
        if (properties.getClusters() != null) {
            properties.getClusters().forEach(c -> clusters.put(c.getId(), c));
        }
        resetLimitsToDefaults();
    }

    /**
     * Лимиты каналов — к значениям по умолчанию, как при старте узла: 100 МБ/с между ЦОД,
     * 60 МБ/с между кластерами HDFS; лимиты для маршрутов вне топологии снимаются.
     *
     * <p>Лимиты в памяти переживают очистку таблицы лимитов: тесты, делящие контекст, видели
     * нулевой лимит (ограждение), оставленный предыдущим тестом, и Kill-Switch считался
     * активным в зависимости от порядка запуска.
     */
    public void resetLimitsToDefaults() {
        dcLimits.clear();
        explicitDcLimits.clear();
        hdfsLimits.clear();

        // Инициализация дефолтных лимитов каналов между ЦОД (100 МБ/с)
        for (var srcDc : datacenters) {
            for (var dstDc : datacenters) {
                if (!srcDc.getId().equalsIgnoreCase(dstDc.getId())) {
                    dcLimits.put(srcDc.getId() + "->" + dstDc.getId(), 100L * 1024 * 1024);
                }
            }
        }

        // Инициализация дефолтных лимитов каналов между кластерами HDFS (60 МБ/с)
        for (var srcCl : clusters.values()) {
            for (var dstCl : clusters.values()) {
                if (!srcCl.getId().equalsIgnoreCase(dstCl.getId())) {
                    setHdfsLimit(srcCl.getId(), dstCl.getId(), 60L * 1024 * 1024);
                }
            }
        }
    }

    public List<ReplicatorProperties.DatacenterConfig> getDatacenters() {
        return Collections.unmodifiableList(datacenters);
    }

    public List<ClusterDto> getClusters() {
        List<ClusterDto> list = new ArrayList<>();
        for (var c : clusters.values()) {
            ClusterDto dto = new ClusterDto();
            dto.setId(c.getId());
            dto.setName(c.getName());
            dto.setDcId(c.getDcId());
            dto.setGrpcAddress(c.getGrpcHost() + ":" + c.getGrpcPort());
            list.add(dto);
        }
        return list;
    }

    public Optional<ReplicatorProperties.ClusterConfig> getCluster(String id) {
        return Optional.ofNullable(clusters.get(id));
    }

    public String getClusterGrpcAddress(String clusterId) {
        if (clusterId == null) return null;
        ReplicatorProperties.ClusterConfig c = clusters.get(clusterId);
        if (c != null && c.getGrpcHost() != null) {
            return c.getGrpcHost() + ":" + c.getGrpcPort();
        }
        return null;
    }

    public void updateClusterGrpcAddress(String clusterId, String grpcAddress) {
        if (clusterId == null || grpcAddress == null || !grpcAddress.contains(":")) return;
        String[] parts = grpcAddress.split(":");
        ReplicatorProperties.ClusterConfig config = clusters.get(clusterId);
        if (config != null) {
            // Защита от Route Hijacking: если в топологии уже сконфигурирован постоянный gRPC шлюз/хост,
            // запрещаем произвольным агентам динамически перезаписывать адрес кластера.
            if (config.getGrpcHost() != null && !config.getGrpcHost().isBlank() && !"localhost".equalsIgnoreCase(config.getGrpcHost())) {
                return;
            }
            config.setGrpcHost(parts[0]);
            try {
                config.setGrpcPort(Integer.parseInt(parts[1]));
            } catch (NumberFormatException ignored) {}
        }
    }

    public void setDcLimit(String srcDc, String dstDc, long limitBytesPerSec) {
        dcLimits.put(srcDc + "->" + dstDc, limitBytesPerSec);
        explicitDcLimits.add(srcDc + "->" + dstDc);
    }

    public Optional<Long> getDcLimit(String srcDc, String dstDc) {
        if (srcDc == null || dstDc == null) return Optional.empty();
        String s = srcDc.trim().toLowerCase();
        String d = dstDc.trim().toLowerCase();
        for (var entry : dcLimits.entrySet()) {
            String[] parts = entry.getKey().split("->");
            if (parts.length == 2) {
                String kSrc = parts[0].trim().toLowerCase();
                String kDst = parts[1].trim().toLowerCase();
                if ((kSrc.equals(s) && kDst.equals(d)) || (kSrc.equals(d) && kDst.equals(s))) {
                    return Optional.of(entry.getValue());
                }
            }
        }
        return Optional.of(100L * 1024 * 1024);
    }

    /**
     * Лимит канала между ЦОД, только если он задан явно.
     *
     * <p>{@link #getDcLimit} без настройки возвращает 100 МБ/с: на это опираются троттлер HDFS
     * и DR. Авторасчет Kafka-задачи брал от этой подстановки 40% и создавал задачу с лимитом
     * 40 МБ/с на всю задачу — для 2 млн событий/с в десятки раз меньше нужного.
     */
    public Optional<Long> getConfiguredDcLimit(String srcDc, String dstDc) {
        if (srcDc == null || dstDc == null) return Optional.empty();
        String s = srcDc.trim().toLowerCase();
        String d = dstDc.trim().toLowerCase();
        for (var entry : dcLimits.entrySet()) {
            if (!explicitDcLimits.contains(entry.getKey())) {
                continue;
            }
            String[] parts = entry.getKey().split("->");
            if (parts.length == 2) {
                String kSrc = parts[0].trim().toLowerCase();
                String kDst = parts[1].trim().toLowerCase();
                if ((kSrc.equals(s) && kDst.equals(d)) || (kSrc.equals(d) && kDst.equals(s))) {
                    return Optional.of(entry.getValue());
                }
            }
        }
        return Optional.empty();
    }

    public void setHdfsLimit(String srcCluster, String dstCluster, long limitBytesPerSec) {
        hdfsLimits.put(srcCluster + "->" + dstCluster, limitBytesPerSec);
    }

    public Optional<Long> getHdfsLimit(String srcCluster, String dstCluster) {
        Long val = hdfsLimits.get(srcCluster + "->" + dstCluster);
        if (val == null) {
            val = hdfsLimits.get(dstCluster + "->" + srcCluster);
        }
        return Optional.ofNullable(val);
    }

    public List<TopologyResponse.DcLimitDto> getDcLimitsList() {
        List<TopologyResponse.DcLimitDto> list = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        // Сначала генерируем пары из зарегистрированных датацентров
        for (var src : datacenters) {
            for (var dst : datacenters) {
                if (src.getId().equalsIgnoreCase(dst.getId())) continue;
                String key = src.getId() + "->" + dst.getId();
                boolean configured = explicitDcLimits.contains(key);
                long bytes = dcLimits.getOrDefault(key, 100L * 1024 * 1024);
                double mb = Math.round((bytes / (1024.0 * 1024.0)) * 100.0) / 100.0;
                list.add(new TopologyResponse.DcLimitDto(src.getId(), dst.getId(), bytes, mb, configured));
                seen.add(key);
            }
        }

        // Добавляем остальные явно сохраненные ключи, если есть
        for (var entry : dcLimits.entrySet()) {
            if (!seen.contains(entry.getKey()) && entry.getKey().contains("->")) {
                String[] parts = entry.getKey().split("->");
                long bytes = entry.getValue();
                double mb = Math.round((bytes / (1024.0 * 1024.0)) * 100.0) / 100.0;
                list.add(new TopologyResponse.DcLimitDto(parts[0], parts[1], bytes, mb, true));
            }
        }
        return list;
    }

    public List<TopologyResponse.HdfsLimitDto> getHdfsLimitsList() {
        List<TopologyResponse.HdfsLimitDto> list = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        // Пары между всеми кластерами
        for (var src : clusters.values()) {
            for (var dst : clusters.values()) {
                if (src.getId().equalsIgnoreCase(dst.getId())) continue;
                String key = src.getId() + "->" + dst.getId();
                long bytes = hdfsLimits.getOrDefault(key, 60L * 1024 * 1024);
                double mb = Math.round((bytes / (1024.0 * 1024.0)) * 100.0) / 100.0;
                list.add(new TopologyResponse.HdfsLimitDto(src.getId(), dst.getId(), bytes, mb));
                seen.add(key);
            }
        }

        for (var entry : hdfsLimits.entrySet()) {
            if (!seen.contains(entry.getKey()) && entry.getKey().contains("->")) {
                String[] parts = entry.getKey().split("->");
                long bytes = entry.getValue();
                double mb = Math.round((bytes / (1024.0 * 1024.0)) * 100.0) / 100.0;
                list.add(new TopologyResponse.HdfsLimitDto(parts[0], parts[1], bytes, mb));
            }
        }
        return list;
    }

    public Map<String, Long> getAllDcLimits() {
        return Collections.unmodifiableMap(dcLimits);
    }

    public Map<String, Long> getAllHdfsLimits() {
        return Collections.unmodifiableMap(hdfsLimits);
    }
}
