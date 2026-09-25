package br.com.spring.batch.partitioner.repository;

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

    public Optional<ReceivedFileDocument> resume(String id, int maxAttempts) {
        return collection.updateAndGet(unfinished(id).and(ReceivedFileFields.ATTEMPTS).lt(maxAttempts), new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.PARTITIONING)
                .inc(ReceivedFileFields.ATTEMPTS, 1));
    }

    public boolean failExhausted(String id, int maxAttempts) {
        return collection.updateFirst(unfinished(id).and(ReceivedFileFields.ATTEMPTS).gte(maxAttempts),
                new Update().set(ReceivedFileFields.STATUS, FileStatus.FAILED));
    }

    public void recordInspection(String id, MovementInfo movement, PartitioningInfo partitioning) {
        collection.update(id, new Update()
                .set(ReceivedFileFields.MOVEMENT, movement)
                .set(ReceivedFileFields.PARTITIONING, partitioning));
    }

    public void relocate(String id, String currentPath) {
        collection.update(id, new Update().set(blob(ReceivedFileFields.CURRENT_PATH), currentPath));
    }

    public void complete(String id) {
        changeStatus(id, FileStatus.COMPLETED);
    }

    public void failPartitioning(String id) {
        changeStatus(id, FileStatus.FAILED_PARTITIONING);
    }

    public void fail(String id) {
        changeStatus(id, FileStatus.FAILED);
    }

    public void requeue(String id) {
        collection.update(id, new Update()
                .set(ReceivedFileFields.STATUS, FileStatus.FAILED_PARTITIONING)
                .set(ReceivedFileFields.ATTEMPTS, 0));
    }

    private void changeStatus(String id, FileStatus status) {
        collection.update(id, new Update().set(ReceivedFileFields.STATUS, status));
    }

    private static Criteria unfinished(String id) {
        return where(ReceivedFileFields.ID).is(id).and(ReceivedFileFields.STATUS).in(FileStatus.UNFINISHED);
    }

    private static Criteria originals() {
        return where(ReceivedFileFields.ROLE).is(FileRole.ORIGINAL);
    }
}
