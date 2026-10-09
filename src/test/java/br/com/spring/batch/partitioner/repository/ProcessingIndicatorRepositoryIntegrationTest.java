package br.com.spring.batch.partitioner.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import br.com.spring.batch.partitioner.model.document.ProcessingIndicatorDocument;
import br.com.spring.batch.partitioner.model.enums.MovementType;
import com.mongodb.client.MongoClients;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.data.repository.core.support.RepositoryComposition.RepositoryFragments;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class ProcessingIndicatorRepositoryIntegrationTest {

    private static final String MOVEMENT_DATE = "2026-10-08";

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7");

    private MongoTemplate mongoTemplate;
    private ProcessingIndicatorRepository repository;

    @BeforeEach
    void setUp() {
        mongoTemplate = MongoTestTemplates.withApplicationConversions(new SimpleMongoClientDatabaseFactory(
                MongoClients.create(MONGO.getConnectionString()), "indicator_test"));
        mongoTemplate.dropCollection(ProcessingIndicatorDocument.COLLECTION);
        repository = new MongoRepositoryFactory(mongoTemplate).getRepository(ProcessingIndicatorRepository.class,
                RepositoryFragments.just(new ProcessingIndicatorRepositoryCustomImpl(mongoTemplate)));
    }

    @Test
    void storesTrailerDataPerMovementTypeUnderTheHeaderDate() {
        repository.upsert(MOVEMENT_DATE, MovementType.ABERTO, "2026-10-09", 1_000_000);
        repository.upsert(MOVEMENT_DATE, MovementType.FECHADO, "2026-10-10", 250_000);

        Document stored = mongoTemplate.findById(MOVEMENT_DATE, Document.class, ProcessingIndicatorDocument.COLLECTION);

        assertThat(stored).containsOnlyKeys("_id", "data_processamento_movimento_aberto", "quantidade_movimento_aberto",
                "data_processamento_movimento_fechado", "quantidade_movimento_fechado");
        assertThat(stored.getString("data_processamento_movimento_aberto")).isEqualTo("2026-10-09");
        assertThat(stored.getLong("quantidade_movimento_aberto")).isEqualTo(1_000_000L);
        assertThat(repository.findById(MOVEMENT_DATE)).get().satisfies(indicator -> {
            assertThat(indicator.fechadoProcessingDate()).isEqualTo("2026-10-10");
            assertThat(indicator.fechadoQuantity()).isEqualTo(250_000L);
            assertThat(indicator.saldoQuantity()).isNull();
        });
    }

    @Test
    void reprocessingTheSameTypeOverwritesItsFields() {
        repository.upsert(MOVEMENT_DATE, MovementType.SALDO, "2026-10-09", 10);
        repository.upsert(MOVEMENT_DATE, MovementType.SALDO, "2026-10-11", 12);

        assertThat(repository.findById(MOVEMENT_DATE)).get().satisfies(indicator -> {
            assertThat(indicator.saldoProcessingDate()).isEqualTo("2026-10-11");
            assertThat(indicator.saldoQuantity()).isEqualTo(12L);
        });
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    void concurrentUpsertsOfDifferentTypesOnTheSameDateKeepEveryType() throws Exception {
        List<Future<?>> upserts = new ArrayList<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (MovementType type : MovementType.values()) {
                upserts.add(executor.submit(() -> repository.upsert(MOVEMENT_DATE, type, "2026-10-09",
                        type.ordinal() + 1L)));
            }
        }
        for (Future<?> upsert : upserts) {
            upsert.get();
        }

        ProcessingIndicatorDocument indicator = repository.findById(MOVEMENT_DATE).orElseThrow();
        assertThat(List.of(indicator.abertoQuantity(), indicator.fechadoQuantity(), indicator.saldoQuantity(),
                indicator.ultimaQuantity())).doesNotContainNull();
        assertThat(repository.count()).isEqualTo(1);
    }
}
