package br.com.spring.batch.partitioner.batch.partition;

import java.util.OptionalLong;

import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.model.partition.ByteRange;

public interface PartitionCopy {

    CopyResult copy(String sourcePath, ByteRange range, PartitionTarget target, ProgressCounter progress);

    record CopyResult(long copiedBytes, OptionalLong verifiedLines) {
    }
}
