package br.com.spring.batch.partitioner.batch.step;

import br.com.spring.batch.partitioner.batch.job.FileJobParameters;
import br.com.spring.batch.partitioner.batch.metrics.StepVolume;

import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;

public record FileStep(StepContribution contribution) {

    public String fileId() {
        return FileJobParameters.fileIdOf(stepExecution());
    }

    public String fileName() {
        return FileJobParameters.fileNameOf(stepExecution().getJobExecution());
    }

    public JobInstance jobInstance() {
        return stepExecution().getJobExecution().getJobInstance();
    }

    public long jobExecutionId() {
        return stepExecution().getJobExecutionId();
    }

    public ExecutionContext executionContext() {
        return stepExecution().getExecutionContext();
    }

    public void recordVolume(StepVolume volume) {
        volume.writeTo(executionContext());
        contribution.incrementWriteCount(volume.lines());
    }

    private StepExecution stepExecution() {
        return contribution.getStepExecution();
    }
}
