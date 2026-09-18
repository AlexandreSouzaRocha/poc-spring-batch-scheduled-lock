package br.com.spring.batch.partitioner.batch.progress;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import br.com.spring.batch.partitioner.config.properties.AppProperties;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.stereotype.Component;

@Component
public class PartitionProgressReporter {

    private static final StructuredLogger log = StructuredLogger.of(PartitionProgressReporter.class, "partition-job");

    private final Map<String, ProgressCounter> files = new ConcurrentHashMap<>();
    private final int intervalSeconds;

    public PartitionProgressReporter(AppProperties properties) {
        this.intervalSeconds = properties.partition().progressIntervalSeconds();
    }

    public ProgressCounter track(String fileId, long executionId, int partitionIndex, long totalBytes) {
        fileCounter(fileId, executionId).addTotal(totalBytes);
        return new ProgressCounter(totalBytes, window(),
                snapshot -> reportPartition(fileId, partitionIndex, snapshot),
                bytes -> advanceFile(fileId, executionId, bytes));
    }

    public void finish(String fileId, long executionId) {
        files.remove(key(fileId, executionId));
    }

    private void advanceFile(String fileId, long executionId, long bytes) {
        fileCounter(fileId, executionId).advance(bytes);
    }

    private ProgressCounter fileCounter(String fileId, long executionId) {
        return files.computeIfAbsent(key(fileId, executionId),
                key -> new ProgressCounter(0, window(), snapshot -> reportFile(fileId, snapshot), bytes -> { }));
    }

    private void reportPartition(String fileId, int partitionIndex, ProgressSnapshot snapshot) {
        log.info("partition.progress").field("fileId", fileId).field("partitionIndex", partitionIndex)
                .field("percent", snapshot.percent()).field("bytes", snapshot.copiedBytes())
                .field("totalBytes", snapshot.totalBytes()).field("mbPerSec", snapshot.megabytesPerSecond())
                .field("etaSec", snapshot.remainingSeconds())
                .log("partição em andamento");
    }

    private void reportFile(String fileId, ProgressSnapshot snapshot) {
        log.info("partition.progress.file").field("fileId", fileId).field("percent", snapshot.percent())
                .field("bytes", snapshot.copiedBytes()).field("totalBytes", snapshot.totalBytes())
                .field("mbPerSec", snapshot.megabytesPerSecond()).field("etaSec", snapshot.remainingSeconds())
                .log("particionamento em andamento");
    }

    private ProgressWindow window() {
        return ProgressWindow.ofSeconds(intervalSeconds);
    }

    private static String key(String fileId, long executionId) {
        return fileId + ":" + executionId;
    }
}
