package br.com.spring.batch.partitioner.repository;

import java.time.Instant;
import java.util.Optional;

import br.com.spring.batch.partitioner.model.document.BlobLocation;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.ReceivedFileFields;
import br.com.spring.batch.partitioner.model.enums.FileStatus;
import com.mongodb.client.MongoClients;
import org.bson.Document;
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
    private static final String FILE_NAME = "MOV_ABERTO_2026.09.24.001.txt";

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7");

    private MongoTemplate mongoTemplate;
    private OriginalFileRepository repository;

    @BeforeEach
    void setUp() {
        mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(
                MongoClients.create(MONGO.getConnectionString()), "intake_test"));
        mongoTemplate.dropCollection(ReceivedFileDocument.COLLECTION);
        repository = new OriginalFileRepository(new ReceivedFileCollection(mongoTemplate));
    }

    @Test
    void registersWithAttemptsAtRootAndWithoutExecutionData() {
        ReceivedFileDocument original = original(FILE_NAME);
        repository.register(original);

        Document stored = mongoTemplate.findById(original.id(), Document.class, ReceivedFileDocument.COLLECTION);

        assertThat(stored.getString(ReceivedFileFields.STATUS)).isEqualTo("PARTITIONING");
        assertThat(stored.getInteger(ReceivedFileFields.ATTEMPTS)).isEqualTo(1);
        assertThat(stored).doesNotContainKey("execution");
        assertThat(stored.get(ReceivedFileFields.AUDIT, Document.class)).doesNotContainKey("completed_at");
    }

    @Test
    void idIsDerivedFromTheFileName() {
        assertThat(original(FILE_NAME).id()).isEqualTo(ReceivedFileDocument.idOf(FILE_NAME));
    }

    @Test
    void secondRegistrationOfSameFileNameFailsWithDuplicateKey() {
        repository.register(original(FILE_NAME));

        assertThatThrownBy(() -> repository.register(original(FILE_NAME)))
                .isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    void interruptedPartitioningIsResumedWithOneMoreAttempt() {
        String id = registered(FILE_NAME);

        Optional<ReceivedFileDocument> resumed = repository.resume(id, MAX_ATTEMPTS);

        assertThat(resumed).get().satisfies(file -> {
            assertThat(file.status()).isEqualTo(FileStatus.PARTITIONING);
            assertThat(file.attempts()).isEqualTo(2);
        });
    }

    @Test
    void failedPartitioningIsResumedAsPartitioning() {
        String id = registered(FILE_NAME);
        repository.failPartitioning(id);

        assertThat(repository.resume(id, MAX_ATTEMPTS)).get()
                .extracting(ReceivedFileDocument::status).isEqualTo(FileStatus.PARTITIONING);
    }

    @Test
    void exhaustedAttemptsAreNotResumedAndBecomeFailed() {
        String id = registered(FILE_NAME);
        attempts(id, MAX_ATTEMPTS);

        assertThat(repository.resume(id, MAX_ATTEMPTS)).isEmpty();
        assertThat(repository.failExhausted(id, MAX_ATTEMPTS)).isTrue();
        assertThat(repository.getById(id).status()).isEqualTo(FileStatus.FAILED);
    }

    @Test
    void failedFileIsOnlyResumedAfterRequeue() {
        String id = registered(FILE_NAME);
        repository.fail(id);

        assertThat(repository.resume(id, MAX_ATTEMPTS)).isEmpty();
        assertThat(repository.failExhausted(id, MAX_ATTEMPTS)).isFalse();

        repository.requeue(id);

        assertThat(repository.getById(id).status()).isEqualTo(FileStatus.FAILED_PARTITIONING);
        assertThat(repository.getById(id).attempts()).isZero();
        assertThat(repository.resume(id, MAX_ATTEMPTS)).get()
                .extracting(ReceivedFileDocument::attempts).isEqualTo(1);
    }

    @Test
    void completedFileIsNeverResumed() {
        String id = registered(FILE_NAME);
        repository.complete(id);

        assertThat(repository.resume(id, MAX_ATTEMPTS)).isEmpty();
        assertThat(repository.getById(id).status()).isEqualTo(FileStatus.COMPLETED);
    }

    @Test
    void unfinishedFilesAreThoseInProgressOrAwaitingRetry() {
        String partitioning = registered("MOV_ABERTO_2026.09.24.001.txt");
        String retry = registered("MOV_FECHADO_2026.09.24.001.txt");
        String failed = registered("MOV_SALDO_2026.09.24.001.txt");
        String completed = registered("MOV_ULTIMA_2026.09.24.001.txt");
        repository.failPartitioning(retry);
        repository.fail(failed);
        repository.complete(completed);

        assertThat(repository.findUnfinished()).extracting(ReceivedFileDocument::id)
                .containsExactlyInAnyOrder(partitioning, retry);
    }

    private String registered(String fileName) {
        ReceivedFileDocument original = original(fileName);
        repository.register(original);
        return original.id();
    }

    private void attempts(String id, int attempts) {
        mongoTemplate.updateFirst(query(where(ReceivedFileFields.ID).is(id)),
                new Update().set(ReceivedFileFields.ATTEMPTS, attempts), ReceivedFileDocument.COLLECTION);
    }

    private static ReceivedFileDocument original(String fileName) {
        return ReceivedFileDocument.original(fileName,
                BlobLocation.received("entrada/" + fileName, "etag", 100), null, Instant.now());
    }
}
