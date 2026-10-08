package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.batch.FileExecutionRecovery;
import br.com.spring.batch.partitioner.batch.FilePartitionJob;
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

    private final JobOperator jobOperator;
    private final Job filePartitionJob;
    private final FileExecutionRecovery recovery;
    private final FileStatusService statusService;

    public FilePartitionJobRunner(JobOperator jobOperator, Job filePartitionJob, FileExecutionRecovery recovery,
                                  FileStatusService statusService) {
        this.jobOperator = jobOperator;
        this.filePartitionJob = filePartitionJob;
        this.recovery = recovery;
        this.statusService = statusService;
    }

    public void run(ReceivedFileDocument file) {
        try {
            start(file);
        } catch (JobInstanceAlreadyCompleteException e) {
            log.warn("file.process").field("fileId", file.id()).field("action", "sync-status")
                    .log("job já estava COMPLETED; status do arquivo sincronizado");
            statusService.completed(file.id());
        } catch (Exception e) {
            failure(file, e);
        }
    }

    private void failure(ReceivedFileDocument file, Exception error) {
        if (DuplicateKeyDetector.isDuplicateKey(error)) {
            log.warn("file.concurrent").field("fileId", file.id()).field("fileName", file.fileName()).error(error)
                    .log("outra instância já iniciou o job deste arquivo; seguindo para o próximo");
            return;
        }
        log.error("file.process").field("fileId", file.id()).error(error).log("falha ao executar o job");
        statusService.failed(file.id(), ErrorSummary.oneLine(error), true);
    }

    private void start(ReceivedFileDocument file) throws Exception {
        boolean restart = recovery.prepareRestart(file);
        log.info("file.process").field("fileId", file.id()).field("fileName", file.fileName())
                .field("attempt", file.attempts()).field("restart", restart)
                .data("status", file.status()).data("sizeBytes", file.sizeBytes())
                .log(restart ? "retomando particionamento de onde parou" : "iniciando particionamento");
        recovery.removeOrphanInstance(file);
        jobOperator.start(filePartitionJob, FilePartitionJob.parametersFor(file.fileName()));
    }
}
