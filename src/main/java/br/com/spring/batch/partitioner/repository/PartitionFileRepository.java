package br.com.spring.batch.partitioner.repository;

import java.util.List;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface PartitionFileRepository extends MongoRepository<ReceivedFileDocument, String>,
        PartitionFileRepositoryCustom {

    List<ReceivedFileDocument> findByParentFileIdOrderByPartitioningIndexAsc(String parentFileId);

    List<ReceivedFileDocument> findByParentFileIdAndAuditPublishedAtIsNullOrderByPartitioningIndexAsc(
            String parentFileId);

    long deleteByParentFileId(String parentFileId);
}
