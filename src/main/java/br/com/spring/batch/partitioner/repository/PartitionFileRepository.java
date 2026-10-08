package br.com.spring.batch.partitioner.repository;

import java.time.Instant;
import java.util.List;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.ReceivedFileFields;
import br.com.spring.batch.partitioner.model.enums.FileStatus;

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import static br.com.spring.batch.partitioner.model.document.ReceivedFileFields.audit;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Repository
public class PartitionFileRepository {

    private static final String COLLECTION = ReceivedFileDocument.COLLECTION;
    private static final String PARTITION_INDEX = ReceivedFileFields.PARTITIONING + "." + ReceivedFileFields.INDEX;
    private static final String PUBLISHED_AT = audit(ReceivedFileFields.PUBLISHED_AT);
    private static final String UPDATED_AT = audit(ReceivedFileFields.UPDATED_AT);

    private final MongoTemplate mongoTemplate;
    private final TransactionTemplate mongoTransaction;

    public PartitionFileRepository(MongoTemplate mongoTemplate, MongoTransactionManager transactionManager) {
        this.mongoTemplate = mongoTemplate;
        this.mongoTransaction = new TransactionTemplate(transactionManager);
    }

    public void replaceAll(String parentFileId, List<ReceivedFileDocument> partitions) {
        mongoTransaction.executeWithoutResult(status -> {
            mongoTemplate.remove(query(ofParent(parentFileId)), COLLECTION);
            mongoTemplate.insert(partitions, COLLECTION);
        });
    }

    public List<ReceivedFileDocument> findByParent(String parentFileId) {
        return find(query(ofParent(parentFileId)));
    }

    public List<ReceivedFileDocument> findUnpublished(String parentFileId) {
        return find(query(ofParent(parentFileId).and(PUBLISHED_AT).isNull()));
    }

    public long deleteByParent(String parentFileId) {
        return mongoTemplate.remove(query(ofParent(parentFileId)), COLLECTION).getDeletedCount();
    }

    public void completePublished(List<String> partitionIds, Instant publishedAt) {
        mongoTemplate.updateMulti(query(where(ReceivedFileFields.ID).in(partitionIds)), new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.COMPLETED)
                .set(PUBLISHED_AT, publishedAt)
                .set(UPDATED_AT, Instant.now()), COLLECTION);
    }

    private List<ReceivedFileDocument> find(Query query) {
        return mongoTemplate.find(query.with(Sort.by(Sort.Direction.ASC, PARTITION_INDEX)),
                ReceivedFileDocument.class, COLLECTION);
    }

    private static Criteria ofParent(String parentFileId) {
        return where(ReceivedFileFields.PARENT_FILE_ID).is(parentFileId);
    }
}
