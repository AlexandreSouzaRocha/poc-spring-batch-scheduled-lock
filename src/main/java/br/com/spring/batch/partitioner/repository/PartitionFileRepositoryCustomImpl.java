package br.com.spring.batch.partitioner.repository;

import java.time.Instant;
import java.util.List;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.ReceivedFileFields;
import br.com.spring.batch.partitioner.model.enums.FileStatus;

import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.transaction.support.TransactionTemplate;

import static br.com.spring.batch.partitioner.model.document.ReceivedFileFields.audit;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class PartitionFileRepositoryCustomImpl implements PartitionFileRepositoryCustom {

    private final MongoTemplate mongoTemplate;
    private final TransactionTemplate mongoTransaction;

    public PartitionFileRepositoryCustomImpl(MongoTemplate mongoTemplate, MongoTransactionManager transactionManager) {
        this.mongoTemplate = mongoTemplate;
        this.mongoTransaction = new TransactionTemplate(transactionManager);
    }

    @Override
    public void replaceAll(String parentFileId, List<ReceivedFileDocument> partitions) {
        mongoTransaction.executeWithoutResult(status -> {
            mongoTemplate.remove(query(where(ReceivedFileFields.PARENT_FILE_ID).is(parentFileId)),
                    ReceivedFileDocument.class);
            mongoTemplate.insertAll(partitions);
        });
    }

    @Override
    public void completePublished(List<String> partitionIds, Instant publishedAt) {
        mongoTemplate.updateMulti(query(where(ReceivedFileFields.ID).in(partitionIds)), new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.COMPLETED)
                .set(audit(ReceivedFileFields.PUBLISHED_AT), publishedAt)
                .set(audit(ReceivedFileFields.UPDATED_AT), Instant.now()), ReceivedFileDocument.class);
    }
}
