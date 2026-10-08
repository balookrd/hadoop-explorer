package org.apache.hadoop.explorer.hdfs.dto.file;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record ChunkedUploadStatusResponse(
    @JsonProperty("upload_id") String uploadId,
    @JsonProperty("received_chunks") List<Integer> receivedChunks,
    @JsonProperty("total_chunks") int totalChunks,
    @JsonProperty("is_complete") boolean isComplete
) {}
