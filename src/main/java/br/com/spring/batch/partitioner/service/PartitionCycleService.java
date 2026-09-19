package br.com.spring.batch.partitioner.service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
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

    public PartitionCycleService(OriginalFileRepository repository, FilePartitionLauncher launcher,
                                 PartitionSettings settings) {
        this.repository = repository;
        this.launcher = launcher;
        this.settings = settings;
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
        log.info("cycle.start").field("files", queue.size()).field("maxConcurrentFiles", settings.maxConcurrentFiles())
                .data("fileIds", queue.files().stream().map(ReceivedFileDocument::id).toList())
                .data("ordem", queue.files().stream().map(ReceivedFileDocument::fileName).toList())
                .log("iniciando ciclo de particionamento");
        dispatch(queue.files());
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

    private void dispatch(List<ReceivedFileDocument> files) {
        Semaphore permits = new Semaphore(settings.maxConcurrentFiles());
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            files.forEach(file -> executor.submit(RequestContext.propagate(() -> launchWithPermit(file, permits))));
        }
    }

    private void launchWithPermit(ReceivedFileDocument file, Semaphore permits) {
        permits.acquireUninterruptibly();
        try {
            RequestContext.run(RequestContext.childRequestId(file.id()), () -> launcher.launch(file));
        } finally {
            permits.release();
        }
    }
}
