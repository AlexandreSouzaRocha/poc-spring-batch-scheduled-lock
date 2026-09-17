package br.com.spring.batch.partitioner.storage;

public interface BlobWriter {

    BlobUpload open(String path);
}
