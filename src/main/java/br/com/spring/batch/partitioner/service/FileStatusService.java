package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.repository.OriginalFileRepository;

import org.springframework.stereotype.Service;

@Service
public class FileStatusService {

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
}
