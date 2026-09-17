package br.com.spring.batch.partitioner.support.log;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.event.Level;
import tools.jackson.databind.json.JsonMapper;

public final class LogEntry {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final LogTarget target;
    private final Map<String, Object> fields = new LinkedHashMap<>();
    private final Map<String, Object> data = new LinkedHashMap<>();

    LogEntry(Logger logger, Level level, String context, String operation) {
        this.target = new LogTarget(logger, level, context, operation);
    }

    public LogEntry field(String name, Object value) {
        fields.put(name, value);
        return this;
    }

    public LogEntry data(String name, Object value) {
        data.put(name, value);
        return this;
    }

    public LogEntry error(Throwable error) {
        data.put("error", ErrorSummary.of(error));
        return this;
    }

    public void log(String message) {
        if (!target.enabled()) {
            return;
        }
        StringBuilder line = new StringBuilder(256)
                .append('[').append(RequestContext.currentRequestId()).append("] ")
                .append(target.context()).append(' ')
                .append(target.operation()).append(' ')
                .append(message);
        fields.forEach((name, value) -> line.append(' ').append(name).append('=').append(format(value)));
        appendData(line);
        target.write(line.toString());
    }

    private void appendData(StringBuilder line) {
        if (data.isEmpty()) {
            return;
        }
        line.append(' ').append(toJson());
    }

    private String toJson() {
        try {
            return JSON.writeValueAsString(data);
        } catch (RuntimeException e) {
            return "{\"json_error\":\"" + e.getClass().getSimpleName() + "\"}";
        }
    }

    private static String format(Object value) {
        String text = String.valueOf(value);
        boolean needsQuotes = text.isEmpty() || text.chars().anyMatch(Character::isWhitespace);
        return needsQuotes ? '"' + text + '"' : text;
    }

    private record LogTarget(Logger logger, Level level, String context, String operation) {

        boolean enabled() {
            return logger.isEnabledForLevel(level);
        }

        void write(String line) {
            logger.atLevel(level).log(line);
        }
    }
}
