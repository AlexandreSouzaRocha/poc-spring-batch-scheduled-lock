package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.batch.heartbeat.FileHeartbeat;
import br.com.spring.batch.partitioner.batch.job.FileJobParameters;
import br.com.spring.batch.partitioner.batch.recovery.AbandonedExecutionRecovery;
import br.com.spring.batch.partitioner.batch.recovery.OrphanJobInstanceCleaner;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.support.DuplicateKeyDetector;
import br.com.spring.batch.partitioner.support.log.ErrorSummary;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.stereotype.Service;

@Service
public class FilePartitionJobRunner {

    private static final StructuredLogger log = StructuredLogger.of(FilePartitionJobRunner.class, "file-partitioning");

    private final JobLauncherGateway gateway;
    private final AbandonedExecutionRecovery recovery;
    private final OrphanJobInstanceCleaner orphanCleaner;
    private final FileStatusService statusService;
    private final FileHeartbeat heartbeat;

    public FilePartitionJobRunner(JobOperator jobOperator, Job filePartitionJob, AbandonedExecutionRecovery recovery,
                                  OrphanJobInstanceCleaner orphanCleaner,
                                  FileStatusService statusService, FileHeartbeat heartbeat) {
        this.gateway = new JobLauncherGateway(jobOperator, filePartitionJob);
        this.recovery = recovery;
        this.orphanCleaner = orphanCleaner;
        this.statusService = statusService;
        this.heartbeat = heartbeat;
    }

    public void run(ReceivedFileDocument file) {
        try {
            start(file);
        } catch (JobInstanceAlreadyCompleteException e) {
            log.warn("file.process").field("fileId", file.id()).field("action", "sync-status")
                    .log("job já estava COMPLETED; status do arquivo sincronizado");
            statusService.completed(file.id(), file.owner(), 0);
        } catch (Exception e) {
            failure(file, e);
        } finally {
            heartbeat.stop(file.id());
        }
    }

    private void failure(ReceivedFileDocument file, Exception error) {
        if (DuplicateKeyDetector.isDuplicateKey(error)) {
            log.warn("file.concurrent").field("fileId", file.id()).field("fileName", file.fileName()).error(error)
                    .log("outra instância já iniciou o job deste arquivo; seguindo para o próximo");
            return;
        }
        log.error("file.process").field("fileId", file.id()).error(error).log("falha ao executar o job");
        statusService.failed(file.id(), file.owner(), ErrorSummary.oneLine(error), true);
    }

    private void start(ReceivedFileDocument file) throws Exception {
        boolean restart = recovery.prepareRestart(file.id());
        log.info("file.process").field("fileId", file.id()).field("fileName", file.fileName())
                .field("attempt", file.attempts()).field("restart", restart)
                .data("status", file.status()).data("sizeBytes", file.sizeBytes())
                .log(restart ? "retomando particionamento de onde parou" : "iniciando particionamento");
        orphanCleaner.removeOrphanOf(file.id());
        gateway.start(file.id());
    }

    private record JobLauncherGateway(JobOperator jobOperator, Job job) {

        void start(String fileId) throws Exception {
            jobOperator.start(job, FileJobParameters.forFile(fileId));
        }
    }
}
