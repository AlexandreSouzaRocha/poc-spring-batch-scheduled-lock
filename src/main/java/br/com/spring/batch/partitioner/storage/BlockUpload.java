package br.com.spring.batch.partitioner.storage;

import br.com.spring.batch.partitioner.model.partition.ByteRange;

public interface BlockUpload extends AutoCloseable {

    void stage(int blockIndex, byte[] data, int length);

    void stageFromUrl(int blockIndex, String sourceUrl, ByteRange sourceRange);

    void commit(int blockCount);

    @Override
    void close();
}
