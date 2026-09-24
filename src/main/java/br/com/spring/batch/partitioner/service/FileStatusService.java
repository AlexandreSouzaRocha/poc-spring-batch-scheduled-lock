package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.enums.FileStatus;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.stereotype.Service;

@Service
public class FileStatusService {

    private static final StructuredLogger log = StructuredLogger.of(FileStatusService.class, "file-partitioning");

    private final OriginalFileRepository originals;
    private final PartitionSettings settings;

    public FileStatusService(OriginalFileRepository originals, PartitionSettings settings) {
        this.originals = originals;
        this.settings = settings;
    }

    public void started(String fileId, long jobInstanceId, long jobExecutionId) {
        if (originals.recordJobExecution(fileId, jobInstanceId, jobExecutionId)) {
            return;
        }
        reportOwnershipLost(fileId, jobExecutionId, "início do job");
    }

    public void completed(String fileId, Long owner, long durationMs) {
        if (originals.complete(fileId, owner, durationMs)) {
            return;
        }
        reportOwnershipLost(fileId, owner, "COMPLETED");
    }

    public void failed(String fileId, Long owner, String error, boolean retryable) {
        ReceivedFileDocument file = originals.getById(fileId);
        FileStatus status = retryable && !file.attemptsExhausted(settings.maxAttempts())
                ? FileStatus.FAILED_PARTITIONING : FileStatus.FAILED;
        if (!markFailure(fileId, owner, status, error)) {
            reportOwnershipLost(fileId, owner, status.name());
            return;
        }
        logFailure(file, status, error, retryable);
    }

    public ReceivedFileDocument requeue(String fileId) {
        ReceivedFileDocument file = originals.getById(fileId);
        requireFailed(file);
        originals.requeue(fileId);
        log.warn("file.requeue").field("fileId", fileId).field("previousStatus", file.status())
                .data("fileName", file.fileName()).data("lastError", file.lastError())
                .log("arquivo devolvido para reprocessamento; tentativas zeradas");
        return originals.getById(fileId);
    }

    private boolean markFailure(String fileId, Long owner, FileStatus status, String error) {
        if (status == FileStatus.FAILED) {
            return originals.fail(fileId, owner, error);
        }
        return originals.failPartitioning(fileId, owner, error);
    }

    private void logFailure(ReceivedFileDocument file, FileStatus status, String error, boolean retryable) {
        if (status == FileStatus.FAILED_PARTITIONING) {
            log.warn("file.fail").field("fileId", file.id()).field("status", status)
                    .field("attempt", file.attempts()).field("maxAttempts", settings.maxAttempts())
                    .data("fileName", file.fileName()).data("error", error)
                    .log("falha no processamento; será retomado no próximo ciclo");
            return;
        }
        log.error("file.fail").field("fileId", file.id()).field("status", status)
                .field("attempt", file.attempts()).field("maxAttempts", settings.maxAttempts())
                .field("retryable", retryable).data("fileName", file.fileName()).data("error", error)
                .log("falha definitiva; arquivo mantido no blob aguardando tratativa manual");
    }

    private static void reportOwnershipLost(String fileId, Long owner, String transition) {
        log.warn("file.ownership.lost").field("fileId", fileId).field("jobExecutionId", owner)
                .field("transition", transition)
                .log("arquivo pertence a outra execução; status não alterado por esta instância");
    }

    private static void requireFailed(ReceivedFileDocument file) {
        if (file.status() == FileStatus.FAILED) {
            return;
        }
        throw new IllegalStateException("arquivo " + file.id() + " está em " + file.status()
                + "; só arquivos em FAILED podem ser devolvidos para reprocessamento");
    }
}
