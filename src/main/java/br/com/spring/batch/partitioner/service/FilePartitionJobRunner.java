package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.batch.job.FileJobParameters;
import br.com.spring.batch.partitioner.batch.recovery.AbandonedExecutionRecovery;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.MongoRetry;
import br.com.spring.batch.partitioner.support.log.ErrorSummary;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.stereotype.Service;

@Service
public class FilePartitionJobRunner {

    private static final StructuredLogger log = StructuredLogger.of(FilePartitionJobRunner.class, "file-partitioning");

    private final JobLauncherGateway gateway;
    private final AbandonedExecutionRecovery recovery;
    private final OriginalFileRepository repository;

    public FilePartitionJobRunner(JobOperator jobOperator, Job filePartitionJob, AbandonedExecutionRecovery recovery,
                                  OriginalFileRepository repository) {
        this.gateway = new JobLauncherGateway(jobOperator, filePartitionJob);
        this.recovery = recovery;
        this.repository = repository;
    }

    public JobOutcome run(ReceivedFileDocument file) {
        try {
            return start(file);
        } catch (JobInstanceAlreadyCompleteException e) {
            log.warn("file.process").field("fileId", file.id()).field("action", "sync-status")
                    .log("job já estava COMPLETED; status do arquivo sincronizado");
            repository.complete(file.id(), 0);
            return JobOutcome.COMPLETED;
        } catch (Exception e) {
            repository.fail(file.id(), ErrorSummary.oneLine(e));
            log.error("file.process").field("fileId", file.id()).error(e).log("falha ao executar o job");
            return JobOutcome.LAUNCH_ERROR;
        }
    }

    private JobOutcome start(ReceivedFileDocument file) throws Exception {
        boolean restart = recovery.prepareRestart(file.id());
        ReceivedFileDocument attempt = repository.startAttempt(file.id());
        log.info("file.process").field("fileId", file.id()).field("fileName", file.fileName())
                .field("attempt", attempt.attempts()).field("restart", restart)
                .data("previousStatus", file.status()).data("sizeBytes", file.sizeBytes())
                .log(restart ? "reiniciando particionamento (resume)" : "iniciando particionamento");
        return JobOutcome.of(gateway.start(file.id()));
    }

    private record JobLauncherGateway(JobOperator jobOperator, Job job) {

        JobExecution start(String fileId) throws Exception {
            return MongoRetry.withRetry(() -> jobOperator.start(job, FileJobParameters.forFile(fileId)));
        }
    }
}
