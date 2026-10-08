package br.com.spring.batch.partitioner.batch;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.step.StepExecution;

public final class FilePartitionJob {

    public static final String NAME = "filePartitionJob";
    public static final String FILE_NAME_PARAMETER = "fileName";

    public static final String VALIDATE_HEADER_STEP = "validateHeaderStep";
    public static final String CLEANUP_PARTITIONS_STEP = "cleanupPartitionsStep";
    public static final String PARTITION_MASTER_STEP = "partitionMasterStep";
    public static final String PARTITION_WORKER_STEP = "partitionWorkerStep";
    public static final String REGISTER_PARTITIONS_STEP = "registerPartitionsStep";
    public static final String PUBLISH_PARTITIONS_STEP = "publishPartitionsStep";
    public static final String MOVE_ORIGINAL_STEP = "moveOriginalStep";

    private FilePartitionJob() {
    }

    public static JobParameters parametersFor(String fileName) {
        return new JobParametersBuilder().addString(FILE_NAME_PARAMETER, fileName).toJobParameters();
    }

    public static String fileNameOf(JobExecution jobExecution) {
        return jobExecution.getJobParameters().getString(FILE_NAME_PARAMETER);
    }

    public static String fileIdOf(JobExecution jobExecution) {
        return ReceivedFileDocument.idOf(fileNameOf(jobExecution));
    }

    public static String fileIdOf(StepExecution stepExecution) {
        return fileIdOf(stepExecution.getJobExecution());
    }

    public static boolean isPartitionWorker(String stepName) {
        return stepName.startsWith(PARTITION_WORKER_STEP + ":");
    }
}
