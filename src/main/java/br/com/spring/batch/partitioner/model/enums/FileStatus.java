package br.com.spring.batch.partitioner.model.enums;

import java.util.List;

public enum FileStatus {
    PARTITIONING,
    REPROCESSING,
    FAILED_PARTITIONING,
    FAILED,
    UPLOADED,
    COMPLETED;

    public static final List<FileStatus> IN_PROGRESS = List.of(PARTITIONING, REPROCESSING);
    public static final List<FileStatus> UNFINISHED = List.of(PARTITIONING, REPROCESSING, FAILED_PARTITIONING);

    public boolean isInProgress() {
        return IN_PROGRESS.contains(this);
    }
}
