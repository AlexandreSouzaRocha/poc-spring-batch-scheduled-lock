package br.com.spring.batch.partitioner.batch.progress;

public record ProgressSnapshot(long copiedBytes, long totalBytes, long elapsedMillis) {

    private static final double MEGABYTE = 1024 * 1024;
    private static final double MILLIS_PER_SECOND = 1000;

    public double percent() {
        return round(percentOf(copiedBytes, totalBytes));
    }

    public double megabytesPerSecond() {
        return round(copiedBytes / MEGABYTE / elapsedSeconds());
    }

    public long remainingSeconds() {
        return Math.round((totalBytes - copiedBytes) / bytesPerSecond());
    }

    private double bytesPerSecond() {
        return Math.max(copiedBytes / elapsedSeconds(), 1);
    }

    private double elapsedSeconds() {
        return Math.max(elapsedMillis, 1) / MILLIS_PER_SECOND;
    }

    private static double percentOf(long value, long total) {
        return value * 100.0 / Math.max(total, 1);
    }

    private static double round(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
