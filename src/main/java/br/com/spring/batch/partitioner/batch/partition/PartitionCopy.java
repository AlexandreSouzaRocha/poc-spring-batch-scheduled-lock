package br.com.spring.batch.partitioner.batch.partition;

import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.model.partition.ByteRange;

public interface PartitionCopy {

    long copy(String sourcePath, ByteRange range, String targetPath, byte[] header, ProgressCounter progress);
}
