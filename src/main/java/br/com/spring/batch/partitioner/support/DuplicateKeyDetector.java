package br.com.spring.batch.partitioner.support;

import java.util.stream.Stream;

import com.mongodb.MongoWriteException;

import org.springframework.dao.DuplicateKeyException;

public final class DuplicateKeyDetector {

    private static final int DUPLICATE_KEY_CODE = 11000;

    private DuplicateKeyDetector() {
    }

    public static boolean isDuplicateKey(Throwable error) {
        return causesOf(error).anyMatch(DuplicateKeyDetector::matches);
    }

    private static Stream<Throwable> causesOf(Throwable error) {
        return Stream.iterate(error, cause -> cause != null, Throwable::getCause);
    }

    private static boolean matches(Throwable cause) {
        if (cause instanceof DuplicateKeyException) {
            return true;
        }
        return cause instanceof MongoWriteException write && write.getError().getCode() == DUPLICATE_KEY_CODE;
    }
}
