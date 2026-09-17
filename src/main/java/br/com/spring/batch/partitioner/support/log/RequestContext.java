package br.com.spring.batch.partitioner.support.log;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.slf4j.MDC;

public final class RequestContext {

    public static final String REQUEST_ID = "request_id";
    private static final String NO_REQUEST_ID = "-";

    private RequestContext() {
    }

    public static String newRequestId(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    public static String childRequestId(String childId) {
        return currentRequestId() + "/" + childId;
    }

    public static String currentRequestId() {
        return Optional.ofNullable(MDC.get(REQUEST_ID)).orElse(NO_REQUEST_ID);
    }

    public static void run(String requestId, Runnable action) {
        String previous = MDC.get(REQUEST_ID);
        MDC.put(REQUEST_ID, requestId);
        try {
            action.run();
        } finally {
            restore(previous);
        }
    }

    public static <T> T call(String requestId, Callable<T> action) throws Exception {
        String previous = MDC.get(REQUEST_ID);
        MDC.put(REQUEST_ID, requestId);
        try {
            return action.call();
        } finally {
            restore(previous);
        }
    }

    public static Runnable propagate(Runnable action) {
        Map<String, String> captured = Optional.ofNullable(MDC.getCopyOfContextMap()).orElse(Map.of());
        return () -> {
            Map<String, String> previous = Optional.ofNullable(MDC.getCopyOfContextMap()).orElse(Map.of());
            MDC.setContextMap(captured);
            try {
                action.run();
            } finally {
                MDC.setContextMap(previous);
            }
        };
    }

    private static void restore(String previous) {
        MDC.remove(REQUEST_ID);
        Optional.ofNullable(previous).ifPresent(value -> MDC.put(REQUEST_ID, value));
    }
}
