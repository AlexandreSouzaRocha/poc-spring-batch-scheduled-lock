package br.com.spring.batch.partitioner.lock;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import br.com.spring.batch.partitioner.config.properties.ShedLockProperties;
import br.com.spring.batch.partitioner.config.properties.ShedLockProperties.FieldNames;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClients;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.SimpleLock;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class ConfigurableMongoLockProviderIntegrationTest {

    private static final String COLLECTION = "custom_scheduler_locks";
    private static final FieldNames CUSTOM_FIELDS = new FieldNames("lock_name", "valid_until", "acquired_at", "owner");

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7");

    private MongoTemplate mongoTemplate;

    @BeforeEach
    void setUp() {
        mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(
                MongoClients.create(MONGO.getConnectionString()), "lock_test"));
        mongoTemplate.setWriteConcern(WriteConcern.MAJORITY);
        mongoTemplate.dropCollection(COLLECTION);
    }

    @ParameterizedTest
    @ValueSource(strings = { "_id", "lock_name" })
    void onlyOneInstanceHoldsTheLock(String nameField) {
        FieldNames fields = new FieldNames(nameField, "valid_until", "acquired_at", "owner");
        ConfigurableMongoLockProvider instanceA = provider(fields, "instance-a");
        ConfigurableMongoLockProvider instanceB = provider(fields, "instance-b");

        Optional<SimpleLock> lockA = instanceA.lock(configuration("partitioning", Duration.ofMinutes(1), Duration.ZERO));
        Optional<SimpleLock> lockB = instanceB.lock(configuration("partitioning", Duration.ofMinutes(1), Duration.ZERO));

        assertThat(lockA).isPresent();
        assertThat(lockB).isEmpty();

        lockA.orElseThrow().unlock();

        assertThat(instanceB.lock(configuration("partitioning", Duration.ofMinutes(1), Duration.ZERO))).isPresent();
    }

    @Test
    void persistsLockWithConfiguredCollectionAndFieldNames() {
        provider(CUSTOM_FIELDS, "instance-a").lock(configuration("polling", Duration.ofMinutes(1), Duration.ZERO));

        List<Document> documents = mongoTemplate.findAll(Document.class, COLLECTION);

        assertThat(documents).hasSize(1);
        assertThat(documents.getFirst())
                .containsEntry("lock_name", "polling")
                .containsEntry("owner", "instance-a")
                .containsKeys("valid_until", "acquired_at")
                .doesNotContainKeys("lockUntil", "lockedAt", "lockedBy");
    }

    @Test
    void keepsLockUntilLockAtLeastForAfterUnlock() {
        ConfigurableMongoLockProvider instanceA = provider(CUSTOM_FIELDS, "instance-a");
        ConfigurableMongoLockProvider instanceB = provider(CUSTOM_FIELDS, "instance-b");

        instanceA.lock(configuration("polling", Duration.ofMinutes(1), Duration.ofMinutes(1))).orElseThrow().unlock();

        assertThat(instanceB.lock(configuration("polling", Duration.ofMinutes(1), Duration.ZERO))).isEmpty();
    }

    @Test
    void onlyOwnerCanExtendTheLock() {
        ConfigurableMongoLockProvider instanceA = provider(CUSTOM_FIELDS, "instance-a");
        SimpleLock lock = instanceA.lock(configuration("partitioning", Duration.ofSeconds(30), Duration.ZERO))
                .orElseThrow();

        Optional<SimpleLock> extended = lock.extend(Duration.ofMinutes(5), Duration.ZERO);

        assertThat(extended).isPresent();
        Document stored = mongoTemplate.findAll(Document.class, COLLECTION).getFirst();
        assertThat(stored.getDate("valid_until").toInstant()).isAfter(Instant.now().plus(Duration.ofMinutes(4)));
    }

    @Test
    void expiredLockCanBeTakenByAnotherInstance() {
        ConfigurableMongoLockProvider instanceA = provider(CUSTOM_FIELDS, "instance-a");
        ConfigurableMongoLockProvider instanceB = provider(CUSTOM_FIELDS, "instance-b");

        instanceA.lock(configuration("partitioning", Duration.ofMillis(200), Duration.ZERO)).orElseThrow();

        assertThat(instanceB.lock(configuration("partitioning", Duration.ofMinutes(1), Duration.ZERO))).isEmpty();
        awaitExpiration();
        assertThat(instanceB.lock(configuration("partitioning", Duration.ofMinutes(1), Duration.ZERO))).isPresent();
    }

    private ConfigurableMongoLockProvider provider(FieldNames fields, String owner) {
        MongoLockStore store = new MongoLockStore(mongoTemplate, new ShedLockProperties(COLLECTION, owner, fields));
        return new ConfigurableMongoLockProvider(store, owner);
    }

    private static LockConfiguration configuration(String name, Duration lockAtMostFor, Duration lockAtLeastFor) {
        return new LockConfiguration(Instant.now(), name, lockAtMostFor, lockAtLeastFor);
    }

    private static void awaitExpiration() {
        try {
            Thread.sleep(Duration.ofMillis(400));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
