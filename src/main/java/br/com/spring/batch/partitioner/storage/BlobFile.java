package br.com.spring.batch.partitioner.storage;

public record BlobFile(String path, long sizeBytes, String etag) {

    public boolean isDataFile() {
        return path.endsWith(BlobPaths.FILE_EXTENSION);
    }

    public String fileName() {
        return BlobPaths.fileNameOf(path);
    }
}
