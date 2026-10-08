package br.com.spring.batch.partitioner.repository;

import java.time.Instant;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.PartitioningInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.ReceivedFileFields;
import br.com.spring.batch.partitioner.model.enums.FileRole;
import br.com.spring.batch.partitioner.model.enums.FileStatus;

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import static br.com.spring.batch.partitioner.model.document.ReceivedFileFields.audit;
import static br.com.spring.batch.partitioner.model.document.ReceivedFileFields.blob;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Repository
public class OriginalFileRepository {

    private static final String COLLECTION = ReceivedFileDocument.COLLECTION;
    private static final String CREATED_AT = audit(ReceivedFileFields.CREATED_AT);
    private static final String UPDATED_AT = audit(ReceivedFileFields.UPDATED_AT);

    private final MongoTemplate mongoTemplate;

    public OriginalFileRepository(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public void register(ReceivedFileDocument original) {
        mongoTemplate.insert(original, COLLECTION);
    }

    public Optional<ReceivedFileDocument> findById(String id) {
        return Optional.ofNullable(mongoTemplate.findById(id, ReceivedFileDocument.class, COLLECTION));
    }

    public ReceivedFileDocument getById(String id) {
        return findById(id).orElseThrow(() -> new IllegalStateException("arquivo " + id + " não encontrado"));
    }

    public List<ReceivedFileDocument> findUnfinished() {
        return mongoTemplate.find(query(originals().and(ReceivedFileFields.STATUS).in(FileStatus.UNFINISHED))
                .with(Sort.by(Sort.Direction.ASC, CREATED_AT)), ReceivedFileDocument.class, COLLECTION);
    }

    public List<ReceivedFileDocument> findRecent(Optional<FileStatus> status, int limit) {
        Criteria criteria = status.map(value -> originals().and(ReceivedFileFields.STATUS).is(value))
                .orElseGet(OriginalFileRepository::originals);
        return mongoTemplate.find(query(criteria).with(Sort.by(Sort.Direction.DESC, CREATED_AT)).limit(limit),
                ReceivedFileDocument.class, COLLECTION);
    }

    public Map<FileStatus, Long> countByStatus() {
        Map<FileStatus, Long> counts = new EnumMap<>(FileStatus.class);
        Arrays.stream(FileStatus.values()).forEach(status -> counts.put(status,
                mongoTemplate.count(query(originals().and(ReceivedFileFields.STATUS).is(status)), COLLECTION)));
        counts.values().removeIf(count -> count == 0);
        return counts;
    }

    public Optional<ReceivedFileDocument> resume(String id, int maxAttempts) {
        Update resume = touched(new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.PARTITIONING)
                .inc(ReceivedFileFields.ATTEMPTS, 1));
        return Optional.ofNullable(mongoTemplate.findAndModify(
                query(unfinished(id).and(ReceivedFileFields.ATTEMPTS).lt(maxAttempts)), resume,
                FindAndModifyOptions.options().returnNew(true), ReceivedFileDocument.class, COLLECTION));
    }

    public boolean failExhausted(String id, int maxAttempts) {
        Update fail = touched(new Update().set(ReceivedFileFields.STATUS, FileStatus.FAILED));
        return mongoTemplate.updateFirst(query(unfinished(id).and(ReceivedFileFields.ATTEMPTS).gte(maxAttempts)),
                fail, COLLECTION).getModifiedCount() > 0;
    }

    public void recordInspection(String id, MovementInfo movement, PartitioningInfo partitioning) {
        update(id, new Update()
                .set(ReceivedFileFields.MOVEMENT, movement)
                .set(ReceivedFileFields.PARTITIONING, partitioning));
    }

    public void relocate(String id, String currentPath) {
        update(id, new Update().set(blob(ReceivedFileFields.CURRENT_PATH), currentPath));
    }

    public void complete(String id) {
        update(id, new Update().set(ReceivedFileFields.STATUS, FileStatus.COMPLETED));
    }

    public void failPartitioning(String id) {
        update(id, new Update().set(ReceivedFileFields.STATUS, FileStatus.FAILED_PARTITIONING));
    }

    public void fail(String id) {
        update(id, new Update().set(ReceivedFileFields.STATUS, FileStatus.FAILED));
    }

    public void requeue(String id) {
        update(id, new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.FAILED_PARTITIONING)
                .set(ReceivedFileFields.ATTEMPTS, 0));
    }

    private void update(String id, Update update) {
        mongoTemplate.updateFirst(query(where(ReceivedFileFields.ID).is(id)), touched(update), COLLECTION);
    }

    private static Update touched(Update update) {
        return update.set(UPDATED_AT, Instant.now());
    }

    private static Criteria unfinished(String id) {
        return where(ReceivedFileFields.ID).is(id).and(ReceivedFileFields.STATUS).in(FileStatus.UNFINISHED);
    }

    private static Criteria originals() {
        return where(ReceivedFileFields.ROLE).is(FileRole.ORIGINAL);
    }
}
