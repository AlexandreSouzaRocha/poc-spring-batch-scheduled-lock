package br.com.spring.batch.partitioner.batch.partition;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.model.partition.PartitionPlan;
import br.com.spring.batch.partitioner.storage.BlobReader;

import org.springframework.stereotype.Component;

@Component
public class FileInspector {

    private static final ByteRange HEADER_RANGE = new ByteRange(0, FileLayout.HEADER_LINE_BYTES);

    private final BlobReader reader;
    private final PartitionSettings settings;

    public FileInspector(BlobReader reader, PartitionSettings settings) {
        this.reader = reader;
        this.settings = settings;
    }

    public FileInspection inspect(ReceivedFileDocument original) {
        FileHeader header = FileHeader.parse(reader.read(original.currentPath(), HEADER_RANGE));
        long lineCount = FileLayout.detailLineCount(original.sizeBytes());
        int partitionCount = PartitionPlan.effectivePartitionCount(lineCount, settings.count());
        return new FileInspection(header, lineCount, partitionCount);
    }
}
