package br.com.spring.batch.partitioner.support;

import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import java.util.Set;
import org.bson.BsonDocument;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.dao.DuplicateKeyException;

import static org.assertj.core.api.Assertions.assertThat;

class DuplicateKeyDetectorTest {

    @Test
    @DisplayName("reconhece a excecao traduzida pelo spring data")
    void detectsSpringDataException() {
        assertThat(DuplicateKeyDetector.isDuplicateKey(
                new IllegalStateException("falha", new DuplicateKeyException("chave duplicada")))).isTrue();
    }

    @Test
    @DisplayName("reconhece o erro 11000 do driver do mongo")
    void detectsDriverError() {
        MongoWriteException error = new MongoWriteException(
                new WriteError(11000, "E11000 duplicate key", new BsonDocument()), new ServerAddress(), Set.of());

        assertThat(DuplicateKeyDetector.isDuplicateKey(new RuntimeException("falha", error))).isTrue();
    }

    @Test
    @DisplayName("nao confunde com outras falhas")
    void ignoresOtherFailures() {
        assertThat(DuplicateKeyDetector.isDuplicateKey(new IllegalStateException("timeout"))).isFalse();
    }
}
