package br.com.spring.batch.partitioner.storage;

public interface BlockUpload extends AutoCloseable {

    void stage(int blockIndex, byte[] data, int length);

    void commit(int blockCount);

    @Override
    void close();
}
