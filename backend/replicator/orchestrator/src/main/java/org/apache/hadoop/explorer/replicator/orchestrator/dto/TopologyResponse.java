package org.apache.hadoop.explorer.replicator.orchestrator.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.explorer.replicator.model.ClusterDto;
import org.apache.hadoop.explorer.replicator.orchestrator.config.ReplicatorProperties;

import java.util.List;

public record TopologyResponse(
    @JsonProperty("datacenters") List<ReplicatorProperties.DatacenterConfig> datacenters,
    @JsonProperty("clusters") List<ClusterDto> clusters,
    @JsonProperty("global_limit_bytes_per_sec") double globalLimitBytesPerSec,
    @JsonProperty("global_limit_mb_per_sec") double globalLimitMbPerSec,
    @JsonProperty("dc_limits") List<DcLimitDto> dcLimits,
    @JsonProperty("hdfs_limits") List<HdfsLimitDto> hdfsLimits
) {

    public record DcLimitDto(
        @JsonProperty("source_dc") String sourceDc,
        @JsonProperty("target_dc") String targetDc,
        @JsonProperty("limit_bytes_per_sec") long limitBytesPerSec,
        @JsonProperty("limit_mb_per_sec") double limitMbPerSec
    ) {}

    public record HdfsLimitDto(
        @JsonProperty("source_cluster") String sourceCluster,
        @JsonProperty("target_cluster") String targetCluster,
        @JsonProperty("limit_bytes_per_sec") long limitBytesPerSec,
        @JsonProperty("limit_mb_per_sec") double limitMbPerSec
    ) {}
}
