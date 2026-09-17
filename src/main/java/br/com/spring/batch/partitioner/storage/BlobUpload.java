package br.com.spring.batch.partitioner.storage;

import java.io.OutputStream;

public interface BlobUpload extends AutoCloseable {

    OutputStream output();

    void commit();

    @Override
    void close();
}
