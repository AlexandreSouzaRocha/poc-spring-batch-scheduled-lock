package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.enums.FileStatus;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.stereotype.Service;

@Service
public class FileStatusService {

    private static final StructuredLogger log = StructuredLogger.of(FileStatusService.class, "file-partitioning");

    private final OriginalFileRepository originals;

    public FileStatusService(OriginalFileRepository originals) {
        this.originals = originals;
    }

    public void started(String fileId, long jobInstanceId, long jobExecutionId) {
        originals.recordJobExecution(fileId, jobInstanceId, jobExecutionId);
    }

    public void completed(String fileId, long durationMs) {
        originals.complete(fileId, durationMs);
    }

    public void failed(String fileId, String error) {
        originals.fail(fileId, error);
    }

    public void rejected(String fileId, String reason) {
        originals.reject(fileId, reason);
    }

    public ReceivedFileDocument requeue(String fileId) {
        ReceivedFileDocument file = originals.getById(fileId);
        requireBlocked(file);
        originals.requeue(fileId);
        log.warn("file.requeue").field("fileId", fileId).field("previousStatus", file.status())
                .data("fileName", file.fileName()).data("lastError", file.execution().lastError())
                .log("arquivo devolvido para a fila; tentativas zeradas");
        return originals.getById(fileId);
    }

    private static void requireBlocked(ReceivedFileDocument file) {
        if (file.status() == FileStatus.ERROR) {
            return;
        }
        throw new IllegalStateException("arquivo " + file.id() + " está em " + file.status()
                + "; só arquivos em ERROR podem ser devolvidos para a fila");
    }
}
