package br.com.spring.batch.partitioner.batch.job;

public final class BatchNames {

    public static final String JOB_NAME = "filePartitionJob";
    public static final String VALIDATE_HEADER_STEP = "validateHeaderStep";
    public static final String CLEANUP_PARTITIONS_STEP = "cleanupPartitionsStep";
    public static final String PARTITION_MASTER_STEP = "partitionMasterStep";
    public static final String PARTITION_WORKER_STEP = "partitionWorkerStep";
    public static final String REGISTER_PARTITIONS_STEP = "registerPartitionsStep";
    public static final String PUBLISH_PARTITIONS_STEP = "publishPartitionsStep";
    public static final String MOVE_ORIGINAL_STEP = "moveOriginalStep";

    private BatchNames() {
    }

    public static boolean isPartitionWorker(String stepName) {
        return stepName.startsWith(PARTITION_WORKER_STEP + ":");
    }
}
