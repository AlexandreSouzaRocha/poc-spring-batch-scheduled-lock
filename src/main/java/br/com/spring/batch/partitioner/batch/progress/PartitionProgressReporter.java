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
        ProgressCounter fileCounter = fileCounter(fileId, executionId);
        fileCounter.addTotal(totalBytes);
        return new ProgressCounter(totalBytes, intervalSeconds,
                snapshot -> reportPartition(fileId, partitionIndex, snapshot), fileCounter);
    }

    public void finish(String fileId, long executionId) {
        files.remove(key(fileId, executionId));
    }

    private ProgressCounter fileCounter(String fileId, long executionId) {
        return files.computeIfAbsent(key(fileId, executionId),
                key -> new ProgressCounter(0, intervalSeconds, snapshot -> reportFile(fileId, snapshot), null));
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

    private static String key(String fileId, long executionId) {
        return fileId + ":" + executionId;
    }
}
