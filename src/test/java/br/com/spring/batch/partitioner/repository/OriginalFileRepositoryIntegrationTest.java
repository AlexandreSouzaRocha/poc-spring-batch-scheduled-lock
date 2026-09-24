package br.com.spring.batch.partitioner.repository;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import br.com.spring.batch.partitioner.model.document.BlobLocation;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.ReceivedFileFields;
import br.com.spring.batch.partitioner.model.enums.FileStatus;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.query.Update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Testcontainers
class OriginalFileRepositoryIntegrationTest {

    private static final int MAX_ATTEMPTS = 3;
    private static final Duration STALE_AFTER = Duration.ofMinutes(2);

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7");

    private MongoTemplate mongoTemplate;
    private OriginalFileRepository repository;

    @BeforeEach
    void setUp() {
        mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(
                MongoClients.create(MONGO.getConnectionString()), "claim_test"));
        mongoTemplate.dropCollection(ReceivedFileDocument.COLLECTION);
        repository = new OriginalFileRepository(new ReceivedFileCollection(mongoTemplate));
    }

    @Test
    void secondRegistrationOfSameFileFailsWithDuplicateKey() {
        repository.register(original("file-1"));

        assertThatThrownBy(() -> repository.register(original("file-1")))
                .isInstanceOf(DuplicateKeyException.class);
        assertThat(repository.getById("file-1").status()).isEqualTo(FileStatus.PARTITIONING);
    }

    @Test
    void activePartitioningIsNotClaimed() {
        repository.register(original("file-1"));

        assertThat(repository.claimForReprocessing("file-1", staleBefore(), MAX_ATTEMPTS)).isEmpty();
    }

    @Test
    void failedPartitioningIsClaimedForReprocessing() {
        repository.register(original("file-1"));
        repository.failPartitioning("file-1", null, "timeout no kafka");

        Optional<ReceivedFileDocument> claimed = repository.claimForReprocessing("file-1", staleBefore(),
                MAX_ATTEMPTS);

        assertThat(claimed).get().satisfies(file -> {
            assertThat(file.status()).isEqualTo(FileStatus.REPROCESSING);
            assertThat(file.attempts()).isEqualTo(2);
        });
    }

    @Test
    void stalePartitioningIsClaimedByExactlyOneInstance() throws Exception {
        repository.register(original("file-1"));
        age("file-1");

        List<Optional<ReceivedFileDocument>> results = concurrently(8,
                () -> repository.claimForReprocessing("file-1", staleBefore(), MAX_ATTEMPTS));

        assertThat(results.stream().filter(Optional::isPresent).count()).isEqualTo(1);
        assertThat(repository.getById("file-1").status()).isEqualTo(FileStatus.REPROCESSING);
        assertThat(repository.getById("file-1").attempts()).isEqualTo(2);
    }

    @Test
    void staleReprocessingIsClaimedAgain() {
        repository.register(original("file-1"));
        repository.failPartitioning("file-1", null, "erro");
        repository.claimForReprocessing("file-1", staleBefore(), MAX_ATTEMPTS);
        age("file-1");

        assertThat(repository.claimForReprocessing("file-1", staleBefore(), MAX_ATTEMPTS)).isPresent();
    }

    @Test
    void staleFileWithExhaustedAttemptsBecomesFailed() {
        repository.register(original("file-1"));
        mongoTemplate.updateFirst(query(where(ReceivedFileFields.ID).is("file-1")),
                new Update().set(ReceivedFileFields.execution(ReceivedFileFields.ATTEMPTS), MAX_ATTEMPTS),
                ReceivedFileDocument.COLLECTION);
        age("file-1");

        assertThat(repository.claimForReprocessing("file-1", staleBefore(), MAX_ATTEMPTS)).isEmpty();
        assertThat(repository.failAbandoned("file-1", staleBefore(), MAX_ATTEMPTS, "tentativas esgotadas")).isTrue();
        assertThat(repository.getById("file-1").status()).isEqualTo(FileStatus.FAILED);
    }

    @Test
    void failedFileIsNeverClaimedUntilRequeued() {
        repository.register(original("file-1"));
        repository.fail("file-1", null, "erro definitivo");
        age("file-1");

        assertThat(repository.claimForReprocessing("file-1", staleBefore(), MAX_ATTEMPTS)).isEmpty();

        repository.requeue("file-1");

        assertThat(repository.getById("file-1").status()).isEqualTo(FileStatus.FAILED_PARTITIONING);
        assertThat(repository.claimForReprocessing("file-1", staleBefore(), MAX_ATTEMPTS)).isPresent();
    }

    @Test
    void heartbeatKeepsFileInProgressAlive() {
        repository.register(original("file-1"));
        repository.recordJobExecution("file-1", 1, 10);
        age("file-1");

        assertThat(repository.heartbeat("file-1", 10)).isTrue();
        assertThat(repository.claimForReprocessing("file-1", staleBefore(), MAX_ATTEMPTS)).isEmpty();
    }

    @Test
    void reclaimRevokesThePreviousOwner() {
        repository.register(original("file-1"));
        repository.recordJobExecution("file-1", 1, 10);
        age("file-1");

        ReceivedFileDocument reclaimed = repository.claimForReprocessing("file-1", staleBefore(), MAX_ATTEMPTS)
                .orElseThrow();

        assertThat(reclaimed.owner()).isNull();
        assertThat(reclaimed.isOwnedBy(10)).isFalse();
        assertThat(repository.heartbeat("file-1", 10)).isFalse();
        assertThat(repository.complete("file-1", 10L, 5)).isFalse();
        assertThat(repository.fail("file-1", 10L, "zumbi")).isFalse();
        assertThat(repository.getById("file-1").status()).isEqualTo(FileStatus.REPROCESSING);
    }

    @Test
    void newOwnerTakesOverAndFinishesAfterReclaim() {
        repository.register(original("file-1"));
        repository.recordJobExecution("file-1", 1, 10);
        age("file-1");
        repository.claimForReprocessing("file-1", staleBefore(), MAX_ATTEMPTS);

        assertThat(repository.recordJobExecution("file-1", 1, 11)).isTrue();
        assertThat(repository.getById("file-1").isOwnedBy(11)).isTrue();
        assertThat(repository.complete("file-1", 10L, 5)).isFalse();
        assertThat(repository.complete("file-1", 11L, 5)).isTrue();
        assertThat(repository.getById("file-1").status()).isEqualTo(FileStatus.COMPLETED);
    }

    @Test
    void onlyOneExecutionBecomesOwner() {
        repository.register(original("file-1"));

        assertThat(repository.recordJobExecution("file-1", 1, 10)).isTrue();
        assertThat(repository.recordJobExecution("file-1", 1, 11)).isFalse();
        assertThat(repository.getById("file-1").owner()).isEqualTo(10L);
    }

    @Test
    void heartbeatDoesNotTouchFinishedFile() {
        repository.register(original("file-1"));
        repository.recordJobExecution("file-1", 1, 10);
        repository.fail("file-1", 10L, "erro");
        age("file-1");
        Instant before = repository.getById("file-1").audit().updatedAt();

        assertThat(repository.heartbeat("file-1", 10)).isFalse();
        assertThat(repository.getById("file-1").audit().updatedAt()).isEqualTo(before);
    }

    @Test
    void unfinishedFilesAreThoseInProgressOrAwaitingRetry() {
        List.of("partitioning", "retry", "failed", "completed").forEach(id -> repository.register(original(id)));
        repository.failPartitioning("retry", null, "erro");
        repository.fail("failed", null, "erro");
        repository.complete("completed", null, 10);

        assertThat(repository.findUnfinished()).extracting(ReceivedFileDocument::id)
                .containsExactlyInAnyOrder("partitioning", "retry");
    }

    private void age(String id) {
        mongoTemplate.updateFirst(query(where(ReceivedFileFields.ID).is(id)),
                new Update().set(ReceivedFileFields.audit(ReceivedFileFields.UPDATED_AT),
                        Instant.now().minus(STALE_AFTER.multipliedBy(2))),
                ReceivedFileDocument.COLLECTION);
    }

    private static Instant staleBefore() {
        return Instant.now().minus(STALE_AFTER);
    }

    private static <T> List<T> concurrently(int instances, Callable<T> action) throws Exception {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<T>> futures = IntStream.range(0, instances).mapToObj(index -> executor.submit(action)).toList();
            return futures.stream().map(OriginalFileRepositoryIntegrationTest::join).toList();
        }
    }

    private static <T> T join(Future<T> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static ReceivedFileDocument original(String id) {
        return ReceivedFileDocument.original(id, "MOV_ABERTO_2026.09.24.001.txt",
                BlobLocation.received("entrada/MOV_ABERTO_2026.09.24.001.txt", "etag", 100), null, Instant.now());
    }
}
