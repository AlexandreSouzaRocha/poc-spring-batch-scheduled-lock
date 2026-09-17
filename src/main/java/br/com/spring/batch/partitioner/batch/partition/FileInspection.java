package br.com.spring.batch.partitioner.batch.partition;

import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.PartitioningInfo;
import br.com.spring.batch.partitioner.model.layout.FileHeader;

public record FileInspection(FileHeader header, long lineCount, int partitionCount) {

    public MovementInfo movement() {
        return MovementInfo.from(header);
    }

    public PartitioningInfo partitioning() {
        return PartitioningInfo.ofOriginal(lineCount, partitionCount);
    }
}
