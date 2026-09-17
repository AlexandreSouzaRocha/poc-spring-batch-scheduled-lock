package br.com.spring.batch.partitioner.batch.recovery;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import br.com.spring.batch.partitioner.batch.job.BatchNames;
import br.com.spring.batch.partitioner.batch.job.FileJobParameters;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

@Component
public class AbandonedExecutionRecovery {

    private static final StructuredLogger log =
            StructuredLogger.of(AbandonedExecutionRecovery.class, "partition-recovery");
    private static final ExitStatus ABANDONED = ExitStatus.FAILED.addExitDescription("abandoned: owner instance died");

    private final JobRepository jobRepository;

    public AbandonedExecutionRecovery(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    public boolean prepareRestart(String fileId) {
        Optional<JobInstance> instance = Optional.ofNullable(
                jobRepository.getJobInstance(BatchNames.JOB_NAME, FileJobParameters.forFile(fileId)));
        instance.flatMap(this::lastExecution).ifPresent(execution -> recover(fileId, execution));
        return instance.isPresent();
    }

    private Optional<JobExecution> lastExecution(JobInstance instance) {
        return jobRepository.getJobExecutions(instance).stream().max(Comparator.comparing(JobExecution::getId));
    }

    private void recover(String fileId, JobExecution execution) {
        List<StepExecution> danglingSteps = execution.getStepExecutions().stream()
                .filter(step -> step.getStatus().isRunning())
                .toList();
        boolean jobWasRunning = execution.getStatus().isRunning();
        if (!jobWasRunning && danglingSteps.isEmpty()) {
            return;
        }
        danglingSteps.forEach(this::markStepAbandoned);
        markJobAbandoned(execution);
        log.warn("execution.recover").field("fileId", fileId).field("jobExecutionId", execution.getId())
                .field("danglingSteps", danglingSteps.size()).field("jobWasRunning", jobWasRunning)
                .log("execução órfã marcada como FAILED para permitir o restart");
    }

    private void markStepAbandoned(StepExecution step) {
        step.setStatus(BatchStatus.FAILED);
        step.setEndTime(LocalDateTime.now());
        step.setExitStatus(ABANDONED);
        jobRepository.update(step);
    }

    private void markJobAbandoned(JobExecution execution) {
        execution.setStatus(BatchStatus.FAILED);
        execution.setEndTime(Optional.ofNullable(execution.getEndTime()).orElseGet(LocalDateTime::now));
        execution.setExitStatus(ABANDONED);
        jobRepository.update(execution);
    }
}
