package br.com.spring.batch.partitioner.support;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

public final class MongoRetry {

    private static final int MAX_ATTEMPTS = 5;
    private static final long BACKOFF_MILLIS = 250L;
    private static final List<String> TRANSIENT_MARKERS =
            List.of("TransientTransactionError", "NoSuchTransaction", "WriteConflict");

    private MongoRetry() {
    }

    public static <T> T withRetry(Callable<T> action) throws Exception {
        return attempt(action, 1);
    }

    private static <T> T attempt(Callable<T> action, int attempt) throws Exception {
        try {
            return action.call();
        } catch (Exception e) {
            rethrowIfFinal(e, attempt);
            Thread.sleep(BACKOFF_MILLIS * attempt);
            return attempt(action, attempt + 1);
        }
    }

    private static void rethrowIfFinal(Exception error, int attempt) throws Exception {
        if (attempt >= MAX_ATTEMPTS || !isTransient(error)) {
            throw error;
        }
    }

    static boolean isTransient(Throwable error) {
        return Stream.iterate(error, cause -> cause != null, Throwable::getCause)
                .map(Throwable::getMessage)
                .filter(message -> message != null)
                .anyMatch(message -> TRANSIENT_MARKERS.stream().anyMatch(message::contains));
    }
}
