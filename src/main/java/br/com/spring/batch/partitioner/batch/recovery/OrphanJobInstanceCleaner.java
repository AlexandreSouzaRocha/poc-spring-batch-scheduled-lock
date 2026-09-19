package br.com.spring.batch.partitioner.batch.recovery;

import br.com.spring.batch.partitioner.batch.job.BatchNames;
import br.com.spring.batch.partitioner.batch.job.FileJobParameters;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.stereotype.Component;

@Component
public class OrphanJobInstanceCleaner {

    private static final StructuredLogger log =
            StructuredLogger.of(OrphanJobInstanceCleaner.class, "partition-recovery");

    private final JobRepository jobRepository;

    public OrphanJobInstanceCleaner(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    public void removeOrphanOf(String fileId) {
        JobInstance instance = jobRepository.getJobInstance(BatchNames.JOB_NAME,
                FileJobParameters.forFile(fileId));
        if (instance == null || !jobRepository.getJobExecutions(instance).isEmpty()) {
            return;
        }
        jobRepository.deleteJobInstance(instance);
        log.warn("instance.orphan").field("fileId", fileId).field("jobInstanceId", instance.getId())
                .log("job instance sem execução removida antes de relançar o job");
    }
}
