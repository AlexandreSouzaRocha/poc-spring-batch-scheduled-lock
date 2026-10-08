package br.com.spring.batch.partitioner.repository;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.enums.FileRole;
import br.com.spring.batch.partitioner.model.enums.FileStatus;

import org.springframework.data.domain.Limit;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface OriginalFileRepository extends MongoRepository<ReceivedFileDocument, String>,
        OriginalFileRepositoryCustom {

    List<ReceivedFileDocument> findByRoleAndStatusInOrderByAuditCreatedAtAsc(FileRole role,
                                                                            Collection<FileStatus> statuses);

    List<ReceivedFileDocument> findByRoleOrderByAuditCreatedAtDesc(FileRole role, Limit limit);

    List<ReceivedFileDocument> findByRoleAndStatusOrderByAuditCreatedAtDesc(FileRole role, FileStatus status,
                                                                           Limit limit);

    long countByRoleAndStatus(FileRole role, FileStatus status);

    default ReceivedFileDocument getById(String id) {
        return findById(id).orElseThrow(() -> new IllegalStateException("arquivo " + id + " não encontrado"));
    }

    default List<ReceivedFileDocument> findUnfinished() {
        return findByRoleAndStatusInOrderByAuditCreatedAtAsc(FileRole.ORIGINAL, FileStatus.UNFINISHED);
    }

    default List<ReceivedFileDocument> findRecent(Optional<FileStatus> status, int limit) {
        return status
                .map(value -> findByRoleAndStatusOrderByAuditCreatedAtDesc(FileRole.ORIGINAL, value, Limit.of(limit)))
                .orElseGet(() -> findByRoleOrderByAuditCreatedAtDesc(FileRole.ORIGINAL, Limit.of(limit)));
    }

    default Map<FileStatus, Long> countByStatus() {
        Map<FileStatus, Long> counts = new EnumMap<>(FileStatus.class);
        Arrays.stream(FileStatus.values())
                .forEach(status -> counts.put(status, countByRoleAndStatus(FileRole.ORIGINAL, status)));
        counts.values().removeIf(count -> count == 0);
        return counts;
    }
}
