package br.com.spring.batch.partitioner.storage;

public interface BlobMover {

    void move(String sourcePath, String targetPath);
}
