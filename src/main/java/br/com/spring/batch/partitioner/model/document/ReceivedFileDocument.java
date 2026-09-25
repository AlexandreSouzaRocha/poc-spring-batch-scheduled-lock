package br.com.spring.batch.partitioner.model.document;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import br.com.spring.batch.partitioner.model.enums.FileRole;
import br.com.spring.batch.partitioner.model.enums.FileStatus;
import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Field;

public record ReceivedFileDocument(
        @Id String id,
        @Field(ReceivedFileFields.ROLE) FileRole role,
        @Field(ReceivedFileFields.PARENT_FILE_ID) String parentFileId,
        @Field(ReceivedFileFields.FILE_NAME) String fileName,
        @Field(ReceivedFileFields.STATUS) FileStatus status,
        @Field(ReceivedFileFields.ATTEMPTS) Integer attempts,
        @Field(ReceivedFileFields.BLOB) BlobLocation blob,
        @Field(ReceivedFileFields.MOVEMENT) MovementInfo movement,
        @Field(ReceivedFileFields.PARTITIONING) PartitioningInfo partitioning,
        @Field(ReceivedFileFields.AUDIT) AuditInfo audit) {

    public static final String COLLECTION = "received_file_management";

    private static final int FIRST_ATTEMPT = 1;

    public static ReceivedFileDocument original(String fileName, BlobLocation blob, MovementInfo movement,
            Instant now) {
        return new ReceivedFileDocument(idOf(fileName), FileRole.ORIGINAL, null, fileName, FileStatus.PARTITIONING,
                FIRST_ATTEMPT, blob, movement, null, AuditInfo.createdAt(now));
    }

    public static String idOf(String fileName) {
        return UUID.nameUUIDFromBytes(fileName.getBytes(StandardCharsets.UTF_8)).toString();
    }

    public ReceivedFileDocument uploadedPartition(PartitionRange range, String fileName, String path,
            long sizeBytes, Instant now) {
        return new ReceivedFileDocument(partitionId(range.index()), FileRole.PARTITION, id, fileName,
                FileStatus.UPLOADED, null, BlobLocation.written(blob.sourcePath(), path, sizeBytes),
                movement, PartitioningInfo.ofPartition(range, partitioning.count()), AuditInfo.createdAt(now));
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

    public boolean attemptsExhausted(int maxAttempts) {
        return attempts >= maxAttempts;
    }

    public boolean hasMovement() {
        return movement != null;
    }

    public boolean isInspected() {
        return partitioning != null;
    }

    public byte[] headerLineBytes(FileLayout layout) {
        return movement.headerLineBytes(layout);
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
