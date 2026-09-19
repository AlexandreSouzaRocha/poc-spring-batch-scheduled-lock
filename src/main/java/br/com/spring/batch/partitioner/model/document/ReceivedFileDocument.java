package br.com.spring.batch.partitioner.model.document;

import java.time.Instant;

import br.com.spring.batch.partitioner.model.enums.FileRole;
import br.com.spring.batch.partitioner.model.enums.FileStatus;
import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;

public record ReceivedFileDocument(
        @Id String id,
        @Field(ReceivedFileFields.ROLE) FileRole role,
        @Field(ReceivedFileFields.PARENT_FILE_ID) String parentFileId,
        @Field(ReceivedFileFields.FILE_NAME) String fileName,
        @Field(ReceivedFileFields.STATUS) FileStatus status,
        @Field(ReceivedFileFields.BLOB) BlobLocation blob,
        @Field(ReceivedFileFields.MOVEMENT) MovementInfo movement,
        @Field(ReceivedFileFields.PARTITIONING) PartitioningInfo partitioning,
        @Field(ReceivedFileFields.EXECUTION) ExecutionInfo execution,
        @Field(ReceivedFileFields.AUDIT) AuditInfo audit) {

    public static final String COLLECTION = "received_file_management";

    public static ReceivedFileDocument original(String id, String fileName, BlobLocation blob, MovementInfo movement,
            Instant now) {
        return new ReceivedFileDocument(id, FileRole.ORIGINAL, null, fileName, FileStatus.PENDING, blob, movement,
                null, ExecutionInfo.notStarted(), AuditInfo.createdAt(now));
    }

    public ReceivedFileDocument uploadedPartition(PartitionRange range, String fileName, String path, Instant now) {
        return new ReceivedFileDocument(partitionId(range.index()), FileRole.PARTITION, id, fileName,
                FileStatus.UPLOADED, BlobLocation.written(blob.sourcePath(), path, range.fileSizeBytes()),
                movement, PartitioningInfo.ofPartition(range, partitioning.count()),
                ExecutionInfo.inheritedFrom(execution), AuditInfo.createdAt(now));
    }

    public String partitionId(int partitionIndex) {
        return id + "-p" + String.format("%04d", partitionIndex);
    }

    public String currentPath() {
        return blob.currentPath();
    }

    public long sizeBytes() {
        return blob.sizeBytes();
    }

    public int attempts() {
        return execution.attempts();
    }

    public String lastError() {
        return execution.lastError();
    }

    public boolean attemptsExhausted(int maxAttempts) {
        return attempts() >= maxAttempts;
    }

    public boolean hasMovement() {
        return movement != null;
    }

    public byte[] headerLineBytes() {
        return movement.headerLineBytes();
    }

    public MovementType movementType() {
        return movement.type();
    }

    public String movementDate() {
        return movement.date();
    }

    public long lineCount() {
        return partitioning.lineCount();
    }

    public int partitionCount() {
        return partitioning.count();
    }
}
