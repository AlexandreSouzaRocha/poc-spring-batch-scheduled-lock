package br.com.spring.batch.partitioner.batch.metrics;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.Optional;

public record Throughput(StepVolume volume, Duration duration) {

    private static final double MILLIS_PER_SECOND = 1000.0;
    private static final double BYTES_PER_MEGABYTE = 1024.0 * 1024.0;

    public static Throughput between(LocalDateTime start, LocalDateTime end, StepVolume volume) {
        LocalDateTime finish = Optional.ofNullable(end).orElseGet(LocalDateTime::now);
        Duration duration = Optional.ofNullable(start).map(begin -> Duration.between(begin, finish))
                .orElse(Duration.ZERO);
        return new Throughput(volume, duration);
    }

    public long durationMs() {
        return duration.toMillis();
    }

    public long linesPerSecond() {
        return seconds() > 0 ? Math.round(volume.lines() / seconds()) : 0;
    }

    public String megabytesPerSecond() {
        double rate = seconds() > 0 ? volume.bytes() / BYTES_PER_MEGABYTE / seconds() : 0;
        return String.format(Locale.ROOT, "%.2f", rate);
    }

    private double seconds() {
        return durationMs() / MILLIS_PER_SECOND;
    }
}
