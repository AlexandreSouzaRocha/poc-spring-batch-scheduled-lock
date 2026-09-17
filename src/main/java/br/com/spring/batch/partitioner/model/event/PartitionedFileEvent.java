package br.com.spring.batch.partitioner.model.event;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PartitionedFileEvent(
        @JsonProperty("movement_type") String movementType,
        @JsonProperty("blob_path") String blobPath,
        @JsonProperty("movement_date") String movementDate) {
}
