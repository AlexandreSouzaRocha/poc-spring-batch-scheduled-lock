package br.com.spring.batch.partitioner.batch.heartbeat;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;
import jakarta.annotation.PreDestroy;

import org.springframework.stereotype.Component;

@Component
public class FileHeartbeat {

    private static final StructuredLogger log = StructuredLogger.of(FileHeartbeat.class, "file-heartbeat");

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().daemon().name("file-heartbeat").factory());
    private final Map<String, ScheduledFuture<?>> beats = new ConcurrentHashMap<>();
    private final OriginalFileRepository repository;
    private final long intervalMillis;

    public FileHeartbeat(OriginalFileRepository repository, PartitionSettings settings) {
        this.repository = repository;
        this.intervalMillis = settings.heartbeatInterval().toMillis();
    }

    public void start(String fileId, long jobExecutionId) {
        stop(fileId);
        beats.put(fileId, scheduler.scheduleWithFixedDelay(() -> beat(fileId, jobExecutionId), intervalMillis,
                intervalMillis, TimeUnit.MILLISECONDS));
    }

    public void stop(String fileId) {
        Optional.ofNullable(beats.remove(fileId)).ifPresent(beat -> beat.cancel(false));
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }

    private void beat(String fileId, long jobExecutionId) {
        try {
            renew(fileId, jobExecutionId);
        } catch (RuntimeException e) {
            log.warn("file.heartbeat").field("fileId", fileId).error(e)
                    .log("falha ao renovar updated_at; nova tentativa no próximo intervalo");
        }
    }

    private void renew(String fileId, long jobExecutionId) {
        if (repository.heartbeat(fileId, jobExecutionId)) {
            return;
        }
        stop(fileId);
        log.warn("file.ownership.lost").field("fileId", fileId).field("jobExecutionId", jobExecutionId)
                .log("arquivo assumido por outra execução; heartbeat encerrado e o job será interrompido no próximo step");
    }
}
