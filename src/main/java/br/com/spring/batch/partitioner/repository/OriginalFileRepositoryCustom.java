package br.com.spring.batch.partitioner.repository;

import java.util.Optional;

import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.PartitioningInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.TrailerInfo;

public interface OriginalFileRepositoryCustom {

    Optional<ReceivedFileDocument> resume(String id, int maxAttempts);

    boolean failExhausted(String id, int maxAttempts);

    void recordInspection(String id, MovementInfo movement, PartitioningInfo partitioning, TrailerInfo trailer);

    void relocate(String id, String currentPath);

    void complete(String id);

    void failPartitioning(String id);

    void fail(String id);

    void requeue(String id);
}
