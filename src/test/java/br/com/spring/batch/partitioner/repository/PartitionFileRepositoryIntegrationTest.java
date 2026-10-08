package br.com.spring.batch.partitioner.repository;

import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import br.com.spring.batch.partitioner.model.document.BlobLocation;
import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.PartitioningInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.enums.FileStatus;
import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;

import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.data.repository.core.support.RepositoryComposition.RepositoryFragments;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class PartitionFileRepositoryIntegrationTest {

    private static final String PARENT = "parent-1";
    private static final String OTHER_PARENT = "parent-2";

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7").withReplicaSet();

    private PartitionFileRepository repository;

    @BeforeEach
    void setUp() {
        SimpleMongoClientDatabaseFactory factory = new SimpleMongoClientDatabaseFactory(
                MongoClients.create(MONGO.getConnectionString()), "partition_test");
        MongoTemplate mongoTemplate = new MongoTemplate(factory);
        mongoTemplate.dropCollection(ReceivedFileDocument.COLLECTION);
        mongoTemplate.createCollection(ReceivedFileDocument.COLLECTION);
        repository = new MongoRepositoryFactory(mongoTemplate).getRepository(PartitionFileRepository.class,
                RepositoryFragments.just(new PartitionFileRepositoryCustomImpl(mongoTemplate,
                        new MongoTransactionManager(factory))));
    }

    @Test
    void replacesPartitionsOfTheParentAndListsThemByIndex() {
        repository.replaceAll(PARENT, partitionsOf(PARENT, 3, 1, 2));
        repository.replaceAll(OTHER_PARENT, partitionsOf(OTHER_PARENT, 1));

        repository.replaceAll(PARENT, partitionsOf(PARENT, 2, 1));

        assertThat(repository.findByParentFileIdOrderByPartitioningIndexAsc(PARENT))
                .extracting(partition -> partition.partitioning().index())
                .containsExactly(1, 2);
        assertThat(repository.findByParentFileIdOrderByPartitioningIndexAsc(OTHER_PARENT)).hasSize(1);
    }

    @Test
    void onlyUnpublishedPartitionsArePendingAndPublishedOnesBecomeCompleted() {
        List<ReceivedFileDocument> partitions = partitionsOf(PARENT, 1, 2, 3);
        repository.replaceAll(PARENT, partitions);

        repository.completePublished(List.of(partitions.get(1).id()), Instant.now());

        assertThat(repository.findByParentFileIdAndAuditPublishedAtIsNullOrderByPartitioningIndexAsc(PARENT))
                .extracting(partition -> partition.partitioning().index())
                .containsExactly(1, 3);
        assertThat(repository.findById(partitions.get(1).id())).get()
                .satisfies(published -> {
                    assertThat(published.status()).isEqualTo(FileStatus.COMPLETED);
                    assertThat(published.audit().publishedAt()).isNotNull();
                });
    }

    @Test
    void deletesOnlyPartitionsOfTheParent() {
        repository.replaceAll(PARENT, partitionsOf(PARENT, 1, 2));
        repository.replaceAll(OTHER_PARENT, partitionsOf(OTHER_PARENT, 1));

        assertThat(repository.deleteByParentFileId(PARENT)).isEqualTo(2);
        assertThat(repository.findByParentFileIdOrderByPartitioningIndexAsc(OTHER_PARENT)).hasSize(1);
    }

    private static List<ReceivedFileDocument> partitionsOf(String parentFileId, int... indexes) {
        ReceivedFileDocument original = new ReceivedFileDocument(parentFileId, null, null, "MOV.txt",
                FileStatus.PARTITIONING, 1, BlobLocation.received("entrada/MOV.txt", "etag", 1000),
                new MovementInfo("H2026-09-16ABERTO ", MovementType.ABERTO, "2026-09-16"),
                PartitioningInfo.ofOriginal(10, indexes.length), null);
        return IntStream.of(indexes)
                .mapToObj(index -> original.uploadedPartition(
                        new PartitionRange(index, index * 10L, 10, new ByteRange(index * 100L, index * 100L + 100)),
                        "MOV_part_" + index + ".txt", "aberto/2026-09-16/" + parentFileId + "/MOV_part_" + index,
                        100, Instant.now()))
                .toList();
    }
}
