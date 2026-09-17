package br.com.spring.batch.partitioner.batch.job;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.step.StepExecution;

public final class FileJobParameters {

    public static final String FILE_ID = "fileId";

    private FileJobParameters() {
    }

    public static JobParameters forFile(String fileId) {
        return new JobParametersBuilder().addString(FILE_ID, fileId).toJobParameters();
    }

    public static String fileIdOf(JobExecution jobExecution) {
        return jobExecution.getJobParameters().getString(FILE_ID);
    }

    public static String fileIdOf(StepExecution stepExecution) {
        return fileIdOf(stepExecution.getJobExecution());
    }
}
