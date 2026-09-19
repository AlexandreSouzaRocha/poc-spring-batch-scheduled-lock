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
import br.com.spring.batch.partitioner.model.queue.ProcessingQueue;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.log.RequestContext;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.stereotype.Service;

@Service
public class PartitionCycleService {

    private static final StructuredLogger log = StructuredLogger.of(PartitionCycleService.class, "file-partitioning");

    private final OriginalFileRepository repository;
    private final FilePartitionLauncher launcher;
    private final PartitionSettings settings;
    private final ProcessingLock processingLock;
    private final SchedulerProperties scheduler;

    public PartitionCycleService(OriginalFileRepository repository, FilePartitionLauncher launcher,
                                 PartitionSettings settings, ProcessingLock processingLock,
                                 SchedulerProperties scheduler) {
        this.repository = repository;
        this.launcher = launcher;
        this.settings = settings;
        this.processingLock = processingLock;
        this.scheduler = scheduler;
    }

    public void processPendingFiles() {
        ProcessingQueue queue = ProcessingQueue.of(repository.findProcessable(), repository.findRejected(),
                settings.filesPerCycle());
        queue.blockedBy().ifPresent(PartitionCycleService::reportBlocked);
        if (queue.isEmpty()) {
            log.debug("cycle.empty").log("nenhum arquivo liberado para processamento");
            return;
        }
        long start = System.currentTimeMillis();
        log.info("cycle.start").field("files", queue.size()).field("maxConcurrentTypes", settings.maxConcurrentTypes())
                .data("fileIds", queue.files().stream().map(ReceivedFileDocument::id).toList())
                .data("ordem", queue.files().stream().map(ReceivedFileDocument::fileName).toList())
                .log("iniciando ciclo de particionamento");
        dispatch(queue.byMovementGroup());
        log.info("cycle.finish").field("files", queue.size()).field("durationMs", System.currentTimeMillis() - start)
                .log("ciclo de particionamento concluído");
    }

    private static void reportBlocked(ReceivedFileDocument blocker) {
        log.warn("cycle.blocked").field("fileId", blocker.id())
                .field("movementType", blocker.movement().type())
                .field("movementDate", blocker.movement().date())
                .data("fileName", blocker.fileName())
                .log("fila bloqueada: arquivos posteriores só serão processados após a resolução deste");
    }

    private void dispatch(Map<String, List<ReceivedFileDocument>> groups) {
        Semaphore permits = new Semaphore(settings.maxConcurrentTypes());
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            groups.forEach((group, files) ->
                    executor.submit(RequestContext.propagate(() -> launchGroup(group, files, permits))));
        }
    }

    private void launchGroup(String group, List<ReceivedFileDocument> files, Semaphore permits) {
        permits.acquireUninterruptibly();
        try {
            processingLock.tryRun(scheduler.lockNameFor(group), () -> files.forEach(this::launch));
        } finally {
            permits.release();
        }
    }

    private void launch(ReceivedFileDocument file) {
        RequestContext.run(RequestContext.childRequestId(file.id()), () -> launcher.launch(file));
    }
}
