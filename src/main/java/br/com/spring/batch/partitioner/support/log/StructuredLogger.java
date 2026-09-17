package br.com.spring.batch.partitioner.support.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public final class StructuredLogger {

    private final Logger logger;
    private final String context;

    private StructuredLogger(Logger logger, String context) {
        this.logger = logger;
        this.context = context;
    }

    public static StructuredLogger of(Class<?> type, String context) {
        return new StructuredLogger(LoggerFactory.getLogger(type), context);
    }

    public LogEntry debug(String operation) {
        return entry(Level.DEBUG, operation);
    }

    public LogEntry info(String operation) {
        return entry(Level.INFO, operation);
    }

    public LogEntry warn(String operation) {
        return entry(Level.WARN, operation);
    }

    public LogEntry error(String operation) {
        return entry(Level.ERROR, operation);
    }

    public LogEntry at(Level level, String operation) {
        return entry(level, operation);
    }

    private LogEntry entry(Level level, String operation) {
        return new LogEntry(logger, level, context, operation);
    }
}
