package br.com.spring.batch.partitioner.repository;

import java.time.Instant;
import java.util.List;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.ReceivedFileFields;
import br.com.spring.batch.partitioner.model.enums.FileStatus;
import jakarta.annotation.PostConstruct;

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import static br.com.spring.batch.partitioner.model.document.ReceivedFileFields.audit;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Repository
public class PartitionFileRepository {

    private static final String PARTITION_INDEX = ReceivedFileFields.PARTITIONING + "." + ReceivedFileFields.INDEX;
    private static final String PUBLISHED_AT = audit(ReceivedFileFields.PUBLISHED_AT);

    private final ReceivedFileCollection collection;
    private final TransactionTemplate mongoTransaction;

    public PartitionFileRepository(ReceivedFileCollection collection, MongoTransactionManager transactionManager) {
        this.collection = collection;
        this.mongoTransaction = new TransactionTemplate(transactionManager);
    }

    @PostConstruct
    void createIndexes() {
        collection.createIndex(new Index()
                .on(ReceivedFileFields.PARENT_FILE_ID, Sort.Direction.ASC)
                .on(PARTITION_INDEX, Sort.Direction.ASC)
                .named("parent_file_id_partition_index"));
    }

    public void replaceAll(String parentFileId, List<ReceivedFileDocument> partitions) {
        mongoTransaction.executeWithoutResult(status -> {
            collection.remove(ofParent(parentFileId));
            collection.insertAll(partitions);
        });
    }

    public List<ReceivedFileDocument> findByParent(String parentFileId) {
        return collection.find(query(ofParent(parentFileId)).with(byIndex()));
    }

    public List<ReceivedFileDocument> findUnpublished(String parentFileId) {
        return collection.find(query(ofParent(parentFileId).and(PUBLISHED_AT).isNull()).with(byIndex()));
    }

    public boolean anyPublished(String parentFileId) {
        return collection.exists(ofParent(parentFileId).and(PUBLISHED_AT).ne(null));
    }

    public long deleteByParent(String parentFileId) {
        return collection.remove(ofParent(parentFileId));
    }

    public void completePublished(List<String> partitionIds, Instant publishedAt) {
        collection.updateAll(where(ReceivedFileFields.ID).in(partitionIds), new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.COMPLETED)
                .set(PUBLISHED_AT, publishedAt)
                .set(audit(ReceivedFileFields.COMPLETED_AT), publishedAt));
    }

    private static Criteria ofParent(String parentFileId) {
        return where(ReceivedFileFields.PARENT_FILE_ID).is(parentFileId);
    }

    private static Sort byIndex() {
        return Sort.by(Sort.Direction.ASC, PARTITION_INDEX);
    }
}
