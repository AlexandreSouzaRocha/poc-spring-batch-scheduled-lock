package br.com.spring.batch.partitioner.repository;

import java.time.Instant;
import java.util.List;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;

public interface PartitionFileRepositoryCustom {

    void replaceAll(String parentFileId, List<ReceivedFileDocument> partitions);

    void completePublished(List<String> partitionIds, Instant publishedAt);
}
