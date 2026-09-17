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
import jakarta.annotation.PostConstruct;

import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import static br.com.spring.batch.partitioner.model.document.ReceivedFileFields.audit;
import static br.com.spring.batch.partitioner.model.document.ReceivedFileFields.blob;
import static br.com.spring.batch.partitioner.model.document.ReceivedFileFields.execution;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Repository
public class OriginalFileRepository {

    private static final List<FileStatus> PROCESSABLE = List.of(FileStatus.PENDING, FileStatus.PARTITIONING,
            FileStatus.FAILED);

    private final ReceivedFileCollection collection;

    public OriginalFileRepository(ReceivedFileCollection collection) {
        this.collection = collection;
    }

    @PostConstruct
    void createIndexes() {
        collection.createIndex(new Index()
                .on(ReceivedFileFields.ROLE, Sort.Direction.ASC)
                .on(ReceivedFileFields.STATUS, Sort.Direction.ASC)
                .on(audit(ReceivedFileFields.CREATED_AT), Sort.Direction.ASC)
                .named("role_status_created_at"));
    }

    public boolean register(ReceivedFileDocument original) {
        return collection.insertIfAbsent(original);
    }

    public Optional<ReceivedFileDocument> findById(String id) {
        return collection.findById(id);
    }

    public ReceivedFileDocument getById(String id) {
        return findById(id).orElseThrow(() -> new IllegalStateException("arquivo " + id + " não encontrado"));
    }

    public List<ReceivedFileDocument> findProcessable(int limit) {
        return collection.find(query(originals().and(ReceivedFileFields.STATUS).in(PROCESSABLE))
                .with(Sort.by(Sort.Direction.ASC, audit(ReceivedFileFields.CREATED_AT)))
                .limit(limit));
    }

    public List<ReceivedFileDocument> findRecent(Optional<FileStatus> status, int limit) {
        Criteria criteria = status.map(value -> originals().and(ReceivedFileFields.STATUS).is(value))
                .orElseGet(OriginalFileRepository::originals);
        return collection.find(query(criteria)
                .with(Sort.by(Sort.Direction.DESC, audit(ReceivedFileFields.CREATED_AT)))
                .limit(limit));
    }

    public Map<FileStatus, Long> countByStatus() {
        Map<FileStatus, Long> counts = new EnumMap<>(FileStatus.class);
        Arrays.stream(FileStatus.values())
                .forEach(status -> counts.put(status, collection.count(originals().and(ReceivedFileFields.STATUS).is(status))));
        counts.values().removeIf(count -> count == 0);
        return counts;
    }

    public ReceivedFileDocument startAttempt(String id) {
        return collection.updateAndGet(id, new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.PARTITIONING)
                .inc(execution(ReceivedFileFields.ATTEMPTS), 1));
    }

    public void recordJobExecution(String id, long jobInstanceId, long jobExecutionId) {
        collection.update(id, new Update()
                .set(execution(ReceivedFileFields.JOB_INSTANCE_ID), jobInstanceId)
                .set(execution(ReceivedFileFields.LAST_JOB_EXECUTION_ID), jobExecutionId));
    }

    public void recordInspection(String id, MovementInfo movement, PartitioningInfo partitioning) {
        collection.update(id, new Update()
                .set(ReceivedFileFields.MOVEMENT, movement)
                .set(ReceivedFileFields.PARTITIONING, partitioning));
    }

    public void relocate(String id, String currentPath) {
        collection.update(id, new Update().set(blob(ReceivedFileFields.CURRENT_PATH), currentPath));
    }

    public void complete(String id, long durationMs) {
        collection.update(id, new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.COMPLETED)
                .set(execution(ReceivedFileFields.DURATION_MS), durationMs)
                .unset(execution(ReceivedFileFields.LAST_ERROR))
                .set(audit(ReceivedFileFields.COMPLETED_AT), Instant.now()));
    }

    public void fail(String id, String error) {
        collection.update(id, new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.FAILED)
                .set(execution(ReceivedFileFields.LAST_ERROR), error));
    }

    public void reject(String id, String error) {
        collection.update(id, new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.ERROR)
                .set(execution(ReceivedFileFields.LAST_ERROR), error));
    }

    private static Criteria originals() {
        return where(ReceivedFileFields.ROLE).is(FileRole.ORIGINAL);
    }
}
