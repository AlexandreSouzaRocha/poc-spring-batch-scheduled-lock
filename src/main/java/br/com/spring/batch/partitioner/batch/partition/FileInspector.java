package br.com.spring.batch.partitioner.batch.partition;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.partition.PartitionPlan;
import br.com.spring.batch.partitioner.storage.BlobHeaderReader;

import org.springframework.stereotype.Component;

@Component
public class FileInspector {

    private final BlobHeaderReader headerReader;
    private final PartitionSettings settings;
    private final FileLayout layout;

    public FileInspector(BlobHeaderReader headerReader, PartitionSettings settings, FileLayout layout) {
        this.headerReader = headerReader;
        this.settings = settings;
        this.layout = layout;
    }

    public FileInspection inspect(ReceivedFileDocument original) {
        FileHeader header = headerReader.read(original.currentPath());
        long lineCount = layout.detailLineCount(original.sizeBytes());
        int partitionCount = PartitionPlan.effectivePartitionCount(lineCount, settings.count());
        return new FileInspection(header, lineCount, partitionCount);
    }
}
