package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.service.dispatch.InboxDispatch;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.stereotype.Service;

@Service
public class PartitionCycleService {

    private static final StructuredLogger log = StructuredLogger.of(PartitionCycleService.class, "file-partitioning");

    private final BlobPollingService pollingService;
    private final InboxDispatch inboxDispatch;
    private final PartitionSettings settings;

    public PartitionCycleService(BlobPollingService pollingService, InboxDispatch inboxDispatch,
                                 PartitionSettings settings) {
        this.pollingService = pollingService;
        this.inboxDispatch = inboxDispatch;
        this.settings = settings;
    }

    public void processInbox() {
        InboxFiles inbox = pollingService.poll();
        if (inbox.isEmpty()) {
            log.debug("inbox.empty").log("nenhum arquivo na entrada");
            return;
        }
        long start = System.currentTimeMillis();
        log.info("inbox.dispatch").field("files", inbox.size())
                .field("concurrencyControl", settings.concurrencyControl())
                .data("fileNames", inbox.fileNames()).log("iniciando ciclo de particionamento");
        inboxDispatch.dispatch(inbox);
        log.info("inbox.finish").field("files", inbox.size()).field("durationMs", System.currentTimeMillis() - start)
                .log("ciclo de particionamento concluído");
    }
}
