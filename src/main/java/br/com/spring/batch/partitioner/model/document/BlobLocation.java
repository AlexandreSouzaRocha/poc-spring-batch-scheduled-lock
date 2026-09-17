package br.com.spring.batch.partitioner.model.document;

import org.springframework.data.mongodb.core.mapping.Field;

public record BlobLocation(
        @Field(ReceivedFileFields.SOURCE_PATH) String sourcePath,
        @Field(ReceivedFileFields.CURRENT_PATH) String currentPath,
        @Field(ReceivedFileFields.ETAG) String etag,
        @Field(ReceivedFileFields.SIZE_BYTES) long sizeBytes) {

    public static BlobLocation received(String path, String etag, long sizeBytes) {
        return new BlobLocation(path, path, etag, sizeBytes);
    }

    public static BlobLocation written(String sourcePath, String path, long sizeBytes) {
        return new BlobLocation(sourcePath, path, null, sizeBytes);
    }
}
