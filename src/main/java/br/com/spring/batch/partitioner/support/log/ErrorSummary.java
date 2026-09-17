package br.com.spring.batch.partitioner.support.log;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class ErrorSummary {

    private static final String APPLICATION_PACKAGE = "br.com.spring.batch.partitioner";

    private ErrorSummary() {
    }

    public static Map<String, Object> of(Throwable error) {
        Throwable root = rootCause(error);
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("type", error.getClass().getName());
        summary.put("message", error.getMessage());
        addRootCause(summary, error, root);
        origin(root).ifPresent(frame -> summary.put("at", describe(frame)));
        return summary;
    }

    public static String oneLine(Throwable error) {
        Throwable root = rootCause(error);
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }

    public static Throwable rootCause(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
    }

    private static void addRootCause(Map<String, Object> summary, Throwable error, Throwable root) {
        if (root == error) {
            return;
        }
        summary.put("root_cause_type", root.getClass().getName());
        summary.put("root_cause_message", root.getMessage());
    }

    private static Optional<StackTraceElement> origin(Throwable error) {
        StackTraceElement[] stack = error.getStackTrace();
        return Arrays.stream(stack)
                .filter(frame -> frame.getClassName().startsWith(APPLICATION_PACKAGE))
                .findFirst()
                .or(() -> Arrays.stream(stack).findFirst());
    }

    private static String describe(StackTraceElement frame) {
        return frame.getClassName() + "." + frame.getMethodName() + ":" + frame.getLineNumber();
    }
}
