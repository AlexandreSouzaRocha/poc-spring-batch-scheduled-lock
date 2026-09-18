package br.com.spring.batch.partitioner.storage;

import java.io.InputStream;
import java.io.OutputStream;

import br.com.spring.batch.partitioner.model.partition.ByteRange;

public interface BlobReader {

    byte[] read(String path, ByteRange range);

    InputStream openStream(String path, ByteRange range);

    long copy(String path, ByteRange range, OutputStream target);

    void copyAll(String path, OutputStream target);
}
