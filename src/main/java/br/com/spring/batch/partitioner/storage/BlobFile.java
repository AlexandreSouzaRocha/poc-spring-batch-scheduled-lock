package br.com.spring.batch.partitioner.storage;

public record BlobFile(String path, long sizeBytes, String etag) {

    private static final String DATA_FILE_EXTENSION = ".dat";

    public boolean isDataFile() {
        return path.endsWith(DATA_FILE_EXTENSION);
    }

    public String fileName() {
        return BlobPaths.fileNameOf(path);
    }
}
