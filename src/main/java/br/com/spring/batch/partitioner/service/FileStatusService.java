package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.repository.PartitionFileRepository;

import org.springframework.stereotype.Service;

@Service
public class FileStatusService {

    private final OriginalFileRepository originals;
    private final PartitionFileRepository partitions;

    public FileStatusService(OriginalFileRepository originals, PartitionFileRepository partitions) {
        this.originals = originals;
        this.partitions = partitions;
    }

    public void started(String fileId, long jobInstanceId, long jobExecutionId) {
        originals.recordJobExecution(fileId, jobInstanceId, jobExecutionId);
    }

    public void completed(String fileId, long durationMs) {
        originals.complete(fileId, durationMs);
        partitions.completeAll(fileId);
    }

    public void failed(String fileId, String error) {
        originals.fail(fileId, error);
    }

    public void rejected(String fileId, String reason) {
        originals.reject(fileId, reason);
    }
}
