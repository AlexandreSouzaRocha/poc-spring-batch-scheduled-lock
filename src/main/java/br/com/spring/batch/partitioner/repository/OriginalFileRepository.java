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

    private final ReceivedFileCollection collection;

    public OriginalFileRepository(ReceivedFileCollection collection) {
        this.collection = collection;
    }

    public void register(ReceivedFileDocument original) {
        collection.insert(original);
    }

    public Optional<ReceivedFileDocument> findById(String id) {
        return collection.findById(id);
    }

    public ReceivedFileDocument getById(String id) {
        return findById(id).orElseThrow(() -> new IllegalStateException("arquivo " + id + " não encontrado"));
    }

    public List<ReceivedFileDocument> findUnfinished() {
        return collection.find(query(originals().and(ReceivedFileFields.STATUS).in(FileStatus.UNFINISHED))
                .with(Sort.by(Sort.Direction.ASC, audit(ReceivedFileFields.CREATED_AT))));
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

    public Optional<ReceivedFileDocument> claimForReprocessing(String id, Instant staleBefore, int maxAttempts) {
        Criteria criteria = byId(id).and(execution(ReceivedFileFields.ATTEMPTS)).lt(maxAttempts)
                .orOperator(where(ReceivedFileFields.STATUS).is(FileStatus.FAILED_PARTITIONING), stale(staleBefore));
        return collection.updateAndGet(criteria, new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.REPROCESSING)
                .inc(execution(ReceivedFileFields.ATTEMPTS), 1)
                .unset(execution(ReceivedFileFields.LAST_JOB_EXECUTION_ID)));
    }

    public boolean failAbandoned(String id, Instant staleBefore, int maxAttempts, String error) {
        Criteria criteria = byId(id).and(execution(ReceivedFileFields.ATTEMPTS)).gte(maxAttempts)
                .andOperator(stale(staleBefore));
        return collection.updateFirst(criteria, new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.FAILED)
                .set(execution(ReceivedFileFields.LAST_ERROR), error)
                .unset(execution(ReceivedFileFields.LAST_JOB_EXECUTION_ID)));
    }

    public boolean heartbeat(String id, long jobExecutionId) {
        return collection.updateFirst(ownedBy(id, jobExecutionId), new Update());
    }

    public boolean recordJobExecution(String id, long jobInstanceId, long jobExecutionId) {
        return collection.updateFirst(ownedBy(id, null), new Update()
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

    public boolean complete(String id, Long owner, long durationMs) {
        return collection.updateFirst(ownedBy(id, owner), new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.COMPLETED)
                .set(execution(ReceivedFileFields.DURATION_MS), durationMs)
                .unset(execution(ReceivedFileFields.LAST_ERROR))
                .set(audit(ReceivedFileFields.COMPLETED_AT), Instant.now()));
    }

    public boolean failPartitioning(String id, Long owner, String error) {
        return markFailure(id, owner, FileStatus.FAILED_PARTITIONING, error);
    }

    public boolean fail(String id, Long owner, String error) {
        return markFailure(id, owner, FileStatus.FAILED, error);
    }

    public void requeue(String id) {
        collection.update(id, new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.FAILED_PARTITIONING)
                .set(execution(ReceivedFileFields.ATTEMPTS), 0)
                .unset(execution(ReceivedFileFields.LAST_ERROR)));
    }

    private boolean markFailure(String id, Long owner, FileStatus status, String error) {
        return collection.updateFirst(ownedBy(id, owner), new Update()
                .set(ReceivedFileFields.STATUS, status)
                .set(execution(ReceivedFileFields.LAST_ERROR), error));
    }

    private static Criteria ownedBy(String id, Long jobExecutionId) {
        return byId(id).and(ReceivedFileFields.STATUS).in(FileStatus.IN_PROGRESS)
                .and(execution(ReceivedFileFields.LAST_JOB_EXECUTION_ID)).is(jobExecutionId);
    }

    private static Criteria stale(Instant staleBefore) {
        return where(ReceivedFileFields.STATUS).in(FileStatus.IN_PROGRESS)
                .and(audit(ReceivedFileFields.UPDATED_AT)).lt(staleBefore);
    }

    private static Criteria byId(String id) {
        return where(ReceivedFileFields.ID).is(id);
    }

    private static Criteria originals() {
        return where(ReceivedFileFields.ROLE).is(FileRole.ORIGINAL);
    }
}
