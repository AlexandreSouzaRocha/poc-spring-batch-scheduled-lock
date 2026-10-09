package br.com.spring.batch.partitioner.repository;

import java.time.Instant;
import java.util.Optional;

import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.PartitioningInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.ReceivedFileFields;
import br.com.spring.batch.partitioner.model.document.TrailerInfo;
import br.com.spring.batch.partitioner.model.enums.FileStatus;

import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;

import static br.com.spring.batch.partitioner.model.document.ReceivedFileFields.audit;
import static br.com.spring.batch.partitioner.model.document.ReceivedFileFields.blob;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class OriginalFileRepositoryCustomImpl implements OriginalFileRepositoryCustom {

    private static final String UPDATED_AT = audit(ReceivedFileFields.UPDATED_AT);

    private final MongoTemplate mongoTemplate;

    public OriginalFileRepositoryCustomImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public Optional<ReceivedFileDocument> resume(String id, int maxAttempts) {
        Update resume = touched(new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.PARTITIONING)
                .inc(ReceivedFileFields.ATTEMPTS, 1));
        return Optional.ofNullable(mongoTemplate.findAndModify(
                query(unfinished(id).and(ReceivedFileFields.ATTEMPTS).lt(maxAttempts)), resume,
                FindAndModifyOptions.options().returnNew(true), ReceivedFileDocument.class));
    }

    @Override
    public boolean failExhausted(String id, int maxAttempts) {
        Update fail = touched(new Update().set(ReceivedFileFields.STATUS, FileStatus.FAILED));
        return mongoTemplate.updateFirst(query(unfinished(id).and(ReceivedFileFields.ATTEMPTS).gte(maxAttempts)),
                fail, ReceivedFileDocument.class).getModifiedCount() > 0;
    }

    @Override
    public void recordInspection(String id, MovementInfo movement, PartitioningInfo partitioning,
                                 TrailerInfo trailer) {
        update(id, new Update()
                .set(ReceivedFileFields.MOVEMENT, movement)
                .set(ReceivedFileFields.PARTITIONING, partitioning)
                .set(ReceivedFileFields.TRAILER, trailer));
    }

    @Override
    public void relocate(String id, String currentPath) {
        update(id, new Update().set(blob(ReceivedFileFields.CURRENT_PATH), currentPath));
    }

    @Override
    public void complete(String id) {
        update(id, new Update().set(ReceivedFileFields.STATUS, FileStatus.COMPLETED));
    }

    @Override
    public void failPartitioning(String id) {
        update(id, new Update().set(ReceivedFileFields.STATUS, FileStatus.FAILED_PARTITIONING));
    }

    @Override
    public void fail(String id) {
        update(id, new Update().set(ReceivedFileFields.STATUS, FileStatus.FAILED));
    }

    @Override
    public void requeue(String id) {
        update(id, new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.FAILED_PARTITIONING)
                .set(ReceivedFileFields.ATTEMPTS, 0));
    }

    private void update(String id, Update update) {
        mongoTemplate.updateFirst(query(where(ReceivedFileFields.ID).is(id)), touched(update),
                ReceivedFileDocument.class);
    }

    private static Update touched(Update update) {
        return update.set(UPDATED_AT, Instant.now());
    }

    private static Criteria unfinished(String id) {
        return where(ReceivedFileFields.ID).is(id).and(ReceivedFileFields.STATUS).in(FileStatus.UNFINISHED);
    }
}
