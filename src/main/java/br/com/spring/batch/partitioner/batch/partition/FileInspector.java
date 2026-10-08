package br.com.spring.batch.partitioner.batch.partition;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.partition.PartitionPlan;
import br.com.spring.batch.partitioner.storage.BlobStorage;

import org.springframework.stereotype.Component;

@Component
public class FileInspector {

    private final BlobStorage storage;
    private final PartitionSettings settings;
    private final FileLayout layout;

    public FileInspector(BlobStorage storage, PartitionSettings settings, FileLayout layout) {
        this.storage = storage;
        this.settings = settings;
        this.layout = layout;
    }

    public FileInspection inspect(ReceivedFileDocument original) {
        FileHeader header = FileHeader.parse(storage.read(original.currentPath(), layout.headerRange()), layout);
        long lineCount = layout.detailLineCount(original.sizeBytes());
        int partitionCount = PartitionPlan.effectivePartitionCount(lineCount, settings.count());
        return new FileInspection(header, lineCount, partitionCount);
    }
}
