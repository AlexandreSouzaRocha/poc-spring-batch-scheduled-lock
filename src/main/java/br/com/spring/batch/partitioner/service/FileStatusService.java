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

    public void completed(String fileId) {
        originals.complete(fileId);
    }

    public void failed(String fileId, String error, boolean retryable) {
        ReceivedFileDocument file = originals.getById(fileId);
        if (retryable && !file.attemptsExhausted(settings.maxAttempts())) {
            originals.failPartitioning(fileId);
            log.warn("file.fail").field("fileId", fileId).field("status", FileStatus.FAILED_PARTITIONING)
                    .field("attempt", file.attempts()).field("maxAttempts", settings.maxAttempts())
                    .data("fileName", file.fileName()).data("error", error)
                    .log("falha no processamento; será retomado no próximo ciclo");
            return;
        }
        originals.fail(fileId);
        log.error("file.fail").field("fileId", fileId).field("status", FileStatus.FAILED)
                .field("attempt", file.attempts()).field("maxAttempts", settings.maxAttempts())
                .field("retryable", retryable).data("fileName", file.fileName()).data("error", error)
                .log("falha definitiva; arquivo mantido no blob aguardando tratativa manual");
    }

    public ReceivedFileDocument requeue(String fileId) {
        ReceivedFileDocument file = originals.getById(fileId);
        requireFailed(file);
        originals.requeue(fileId);
        log.warn("file.requeue").field("fileId", fileId).field("previousStatus", file.status())
                .data("fileName", file.fileName()).log("arquivo devolvido para reprocessamento; tentativas zeradas");
        return originals.getById(fileId);
    }

    private static void requireFailed(ReceivedFileDocument file) {
        if (file.status() == FileStatus.FAILED) {
            return;
        }
        throw new IllegalStateException("arquivo " + file.id() + " está em " + file.status()
                + "; só arquivos em FAILED podem ser devolvidos para reprocessamento");
    }
}
