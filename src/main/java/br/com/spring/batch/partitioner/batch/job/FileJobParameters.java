package br.com.spring.batch.partitioner.batch.job;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.step.StepExecution;

public final class FileJobParameters {

    public static final String FILE_NAME = "fileName";

    private FileJobParameters() {
    }

    public static JobParameters forFile(String fileName) {
        return new JobParametersBuilder().addString(FILE_NAME, fileName).toJobParameters();
    }

    public static String fileNameOf(JobExecution jobExecution) {
        return jobExecution.getJobParameters().getString(FILE_NAME);
    }

    public static String fileIdOf(JobExecution jobExecution) {
        return ReceivedFileDocument.idOf(fileNameOf(jobExecution));
    }

    public static String fileIdOf(StepExecution stepExecution) {
        return fileIdOf(stepExecution.getJobExecution());
    }
}
