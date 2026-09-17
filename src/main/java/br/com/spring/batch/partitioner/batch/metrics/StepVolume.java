package br.com.spring.batch.partitioner.batch.metrics;

import org.springframework.batch.infrastructure.item.ExecutionContext;

public record StepVolume(long lines, long bytes) {

    private static final String LINES = "metrics.lines";
    private static final String BYTES = "metrics.bytes";
    private static final StepVolume EMPTY = new StepVolume(0, 0);

    public static StepVolume empty() {
        return EMPTY;
    }

    public static StepVolume ofLines(long lines) {
        return new StepVolume(lines, 0);
    }

    public static StepVolume ofBytes(long bytes) {
        return new StepVolume(0, bytes);
    }

    public static StepVolume readFrom(ExecutionContext context) {
        return new StepVolume(context.getLong(LINES, 0L), context.getLong(BYTES, 0L));
    }

    public void writeTo(ExecutionContext context) {
        context.putLong(LINES, lines);
        context.putLong(BYTES, bytes);
    }

    public StepVolume plus(StepVolume other) {
        return new StepVolume(lines + other.lines, bytes + other.bytes);
    }
}
