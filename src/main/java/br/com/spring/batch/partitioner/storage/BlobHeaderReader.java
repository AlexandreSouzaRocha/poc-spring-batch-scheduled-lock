package br.com.spring.batch.partitioner.storage;

import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.partition.ByteRange;

import org.springframework.stereotype.Component;

@Component
public class BlobHeaderReader {

    private static final ByteRange HEADER_RANGE = new ByteRange(0, FileLayout.HEADER_LINE_BYTES);

    private final BlobReader reader;

    public BlobHeaderReader(BlobReader reader) {
        this.reader = reader;
    }

    public FileHeader read(String path) {
        return FileHeader.parse(reader.read(path, HEADER_RANGE));
    }
}
