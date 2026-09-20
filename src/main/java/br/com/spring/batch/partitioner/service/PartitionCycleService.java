package br.com.spring.batch.partitioner.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.config.properties.SchedulerProperties;
import br.com.spring.batch.partitioner.lock.ProcessingLock;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.queue.MovementDependencies;
import br.com.spring.batch.partitioner.model.queue.ProcessingQueue;
import br.com.spring.batch.partitioner.service.dispatch.QueueDispatch;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.log.RequestContext;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.stereotype.Service;

@Service
public class PartitionCycleService {

    private static final StructuredLogger log = StructuredLogger.of(PartitionCycleService.class, "file-partitioning");

    private final OriginalFileRepository repository;
    private final PartitionSettings settings;
    private final MovementDependencies dependencies;
    private final QueueDispatch queueDispatch;

    public PartitionCycleService(OriginalFileRepository repository, PartitionSettings settings,
                                 MovementDependencies dependencies, QueueDispatch queueDispatch) {
        this.repository = repository;
        this.settings = settings;
        this.dependencies = dependencies;
        this.queueDispatch = queueDispatch;
    }

    public void processPendingFiles() {
        ProcessingQueue queue = ProcessingQueue.of(repository.findProcessable(), repository.findRejected(),
                dependencies, settings.filesPerCycle());
        queue.blockedBy().ifPresent(PartitionCycleService::reportBlocked);
        if (queue.isEmpty()) {
            log.debug("queue.empty").log("nenhum arquivo liberado para processamento");
            return;
        }
        long start = System.currentTimeMillis();
        log.info("queue.dispatch").field("files", queue.size()).field("dispatch", settings.dispatch())
                .field("data", queue.waveDate().orElse("-"))
                .data("fileIds", queue.files().stream().map(ReceivedFileDocument::id).toList())
                .data("ordem", queue.files().stream().map(ReceivedFileDocument::fileName).toList())
                .log("iniciando ciclo de particionamento");
        queueDispatch.dispatch(queue);
        log.info("queue.finish").field("files", queue.size()).field("durationMs", System.currentTimeMillis() - start)
                .log("ciclo de particionamento concluído");
    }

    private static void reportBlocked(ReceivedFileDocument blocker) {
        log.warn("cycle.blocked").field("fileId", blocker.id())
                .field("movementType", blocker.movement().type())
                .field("movementDate", blocker.movement().date())
                .data("fileName", blocker.fileName())
                .log("fila bloqueada: arquivos posteriores só serão processados após a resolução deste");
    }

}
