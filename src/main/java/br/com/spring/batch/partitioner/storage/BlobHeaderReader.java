package br.com.spring.batch.partitioner.storage;

import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.partition.ByteRange;

import org.springframework.stereotype.Component;

@Component
public class BlobHeaderReader {

    private final BlobReader reader;
    private final FileLayout layout;

    public BlobHeaderReader(BlobReader reader, FileLayout layout) {
        this.reader = reader;
        this.layout = layout;
    }

    public FileHeader read(String path) {
        return FileHeader.parse(reader.read(path, new ByteRange(0, layout.headerLineBytes())), layout);
    }
}
