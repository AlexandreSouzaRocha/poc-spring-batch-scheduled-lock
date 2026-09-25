package br.com.spring.batch.partitioner.model.enums;

import java.util.List;

public enum FileStatus {
    PARTITIONING,
    FAILED_PARTITIONING,
    FAILED,
    UPLOADED,
    COMPLETED;

    public static final List<FileStatus> UNFINISHED = List.of(PARTITIONING, FAILED_PARTITIONING);
}
