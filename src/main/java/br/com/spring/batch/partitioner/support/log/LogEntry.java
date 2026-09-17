package br.com.spring.batch.partitioner.support.log;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.event.Level;
import tools.jackson.databind.json.JsonMapper;

public final class LogEntry {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String CONTEXT = "context";
    private static final String OPERATION = "operation";
    private static final String MESSAGE = "msg";
    private static final String DATA = "data";
    private static final String ERROR = "ex";

    private final LogTarget target;
    private final Map<String, Object> fields = new LinkedHashMap<>();
    private final Map<String, Object> data = new LinkedHashMap<>();
    private Map<String, Object> error;

    LogEntry(Logger logger, Level level, String context, String operation) {
        this.target = new LogTarget(logger, level);
        fields.put(CONTEXT, context);
        fields.put(OPERATION, operation);
    }

    public LogEntry field(String name, Object value) {
        fields.put(name, value);
        return this;
    }

    public LogEntry data(String name, Object value) {
        data.put(name, value);
        return this;
    }

    public LogEntry error(Throwable throwable) {
        error = ErrorSummary.of(throwable);
        return this;
    }

    public void log(String message) {
        if (!target.enabled()) {
            return;
        }
        StringBuilder line = new StringBuilder(256);
        fields.forEach((name, value) -> append(line, name, LogValues.format(value)));
        append(line, MESSAGE, LogValues.quote(message));
        appendJson(line, DATA, data);
        appendJson(line, ERROR, error);
        target.write(line.toString());
    }

    private void appendJson(StringBuilder line, String name, Map<String, Object> json) {
        if (json == null || json.isEmpty()) {
            return;
        }
        append(line, name, toJson(json));
    }

    private static void append(StringBuilder line, String name, String value) {
        if (!line.isEmpty()) {
            line.append(' ');
        }
        line.append(name).append('=').append(value);
    }

    private static String toJson(Map<String, Object> json) {
        try {
            return JSON.writeValueAsString(json);
        } catch (RuntimeException e) {
            return "{\"json_error\":\"" + e.getClass().getSimpleName() + "\"}";
        }
    }

    private record LogTarget(Logger logger, Level level) {

        boolean enabled() {
            return logger.isEnabledForLevel(level);
        }

        void write(String line) {
            logger.atLevel(level).log(line);
        }
    }
}
