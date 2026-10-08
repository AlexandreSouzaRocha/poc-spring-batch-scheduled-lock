package br.com.spring.batch.partitioner.service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import br.com.spring.batch.partitioner.config.properties.AppProperties.ConcurrencyControl;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.lock.ProcessingLock;
import br.com.spring.batch.partitioner.model.layout.MovementFileName;
import br.com.spring.batch.partitioner.storage.BlobFile;
import br.com.spring.batch.partitioner.support.log.RequestContext;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.stereotype.Service;

@Service
public class PartitionCycleService {

    static final String UNKNOWN_GROUP = "desconhecido";

    private static final StructuredLogger log = StructuredLogger.of(PartitionCycleService.class, "file-partitioning");

    private final FileIntakeService intakeService;
    private final FilePartitionJobRunner jobRunner;
    private final ProcessingLock processingLock;
    private final PartitionSettings settings;

    public PartitionCycleService(FileIntakeService intakeService, FilePartitionJobRunner jobRunner,
                                 ProcessingLock processingLock, PartitionSettings settings) {
        this.intakeService = intakeService;
        this.jobRunner = jobRunner;
        this.processingLock = processingLock;
        this.settings = settings;
    }

    public void processInbox() {
        List<BlobFile> files = intakeService.pendingFiles();
        if (files.isEmpty()) {
            log.debug("inbox.empty").log("nenhum arquivo na entrada");
            return;
        }
        long start = System.currentTimeMillis();
        log.info("inbox.dispatch").field("files", files.size())
                .field("concurrencyControl", settings.concurrencyControl())
                .data("fileNames", files.stream().map(BlobFile::fileName).toList())
                .log("iniciando ciclo de particionamento");
        dispatch(files);
        log.info("inbox.finish").field("files", files.size()).field("durationMs", System.currentTimeMillis() - start)
                .log("ciclo de particionamento concluído");
    }

    private void dispatch(List<BlobFile> files) {
        if (settings.concurrencyControl() == ConcurrencyControl.GLOBAL_LOCK) {
            processingLock.tryRunGlobal(() -> files.forEach(this::launch));
            return;
        }
        Semaphore permits = new Semaphore(settings.maxConcurrentTypes());
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            byMovementGroup(files).forEach((group, groupFiles) ->
                    executor.submit(RequestContext.propagate(() -> launchGroup(group, groupFiles, permits))));
        }
    }

    private void launchGroup(String group, List<BlobFile> files, Semaphore permits) {
        permits.acquireUninterruptibly();
        try {
            processingLock.tryRunForType(group, () -> files.forEach(this::launch));
        } finally {
            permits.release();
        }
    }

    private void launch(BlobFile file) {
        RequestContext.run(RequestContext.childRequestId(file.fileId()),
                () -> intakeService.take(file).ifPresent(jobRunner::run));
    }

    static Map<String, List<BlobFile>> byMovementGroup(List<BlobFile> files) {
        return files.stream().collect(Collectors.groupingBy(PartitionCycleService::groupOf, LinkedHashMap::new,
                Collectors.toList()));
    }

    private static String groupOf(BlobFile file) {
        return MovementFileName.parse(file.fileName())
                .map(name -> name.type().name().toLowerCase(Locale.ROOT))
                .orElse(UNKNOWN_GROUP);
    }
}
