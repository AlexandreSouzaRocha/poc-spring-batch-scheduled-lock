package br.com.spring.batch.partitioner.service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
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
        List<ReceivedFileDocument> files = repository.findProcessable(settings.filesPerCycle());
        if (files.isEmpty()) {
            log.debug("cycle.empty").log("nenhum arquivo pendente");
            return;
        }
        long start = System.currentTimeMillis();
        log.info("cycle.start").field("files", files.size()).field("maxConcurrentFiles", settings.maxConcurrentFiles())
                .data("fileIds", files.stream().map(ReceivedFileDocument::id).toList())
                .log("iniciando ciclo de particionamento");
        dispatch(files);
        log.info("cycle.finish").field("files", files.size()).field("durationMs", System.currentTimeMillis() - start)
                .log("ciclo de particionamento concluído");
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
