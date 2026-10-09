package br.com.spring.batch.partitioner.support.chaos;

import java.util.Arrays;
import java.util.Optional;

import br.com.spring.batch.partitioner.batch.FilePartitionJob;

public enum ChaosPoint {
    VALIDATE(FilePartitionJob.VALIDATE_FILE_STEP, false),
    CLEANUP(FilePartitionJob.CLEANUP_PARTITIONS_STEP, false),
    PARTITION(FilePartitionJob.PARTITION_WORKER_STEP, true),
    REGISTER(FilePartitionJob.REGISTER_PARTITIONS_STEP, false),
    MOVE(FilePartitionJob.MOVE_ORIGINAL_STEP, false),
    PUBLISH(FilePartitionJob.PUBLISH_PARTITIONS_STEP, false),
    FINISH(FilePartitionJob.FINISH_FILE_STEP, false);

    private static final String PARTITION_SUFFIX_SEPARATOR = ":";

    private final String stepName;
    private final boolean afterStep;

    ChaosPoint(String stepName, boolean afterStep) {
        this.stepName = stepName;
        this.afterStep = afterStep;
    }

    public boolean afterStep() {
        return afterStep;
    }

    public static Optional<ChaosPoint> ofStep(String stepName) {
        String baseName = stepName.split(PARTITION_SUFFIX_SEPARATOR)[0];
        return Arrays.stream(values()).filter(point -> point.stepName.equals(baseName)).findFirst();
    }
}
