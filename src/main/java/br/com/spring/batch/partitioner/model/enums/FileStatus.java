package br.com.spring.batch.partitioner.model.enums;

public enum FileStatus {
    PENDING,
    PARTITIONING,
    FAILED,
    ERROR,
    UPLOADED,
    COMPLETED
}
