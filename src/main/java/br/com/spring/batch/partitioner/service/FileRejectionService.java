package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.batch.partition.OriginalFileArchiver;
import br.com.spring.batch.partitioner.batch.partition.PartitionCleaner;
import br.com.spring.batch.partitioner.batch.partition.PartitionCleaner.CleanupResult;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.stereotype.Service;

@Service
public class FileRejectionService {

    private static final StructuredLogger log = StructuredLogger.of(FileRejectionService.class, "file-partitioning");

    private final OriginalFileRepository repository;
    private final OriginalFileArchiver archiver;
    private final PartitionCleaner cleaner;

    public FileRejectionService(OriginalFileRepository repository, OriginalFileArchiver archiver,
                                PartitionCleaner cleaner) {
        this.repository = repository;
        this.archiver = archiver;
        this.cleaner = cleaner;
    }

    public void reject(String fileId, String reason) {
        ReceivedFileDocument file = repository.getById(fileId);
        String fullReason = reason + describeLastError(file);
        String errorPath = archiver.moveToError(file);
        CleanupResult cleanup = cleaner.cleanIfUnpublished(file);
        repository.reject(fileId, fullReason);
        log.error("file.reject").field("fileId", fileId).field("fileName", file.fileName())
                .field("attempts", file.attempts()).field("deletedPartitionBlobs", cleanup.deletedBlobs())
                .field("partitionsKept", cleanup.skipped()).data("reason", fullReason).data("blobPath", errorPath)
                .log("arquivo movido para a pasta de erros e não será mais reprocessado");
    }

    private static String describeLastError(ReceivedFileDocument file) {
        return file.lastError() == null ? "" : " | último erro: " + file.lastError();
    }
}
