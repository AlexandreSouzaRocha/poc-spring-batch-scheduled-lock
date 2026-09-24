package br.com.spring.batch.partitioner.service;

import java.util.List;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.storage.BlobCatalog;
import br.com.spring.batch.partitioner.storage.BlobFile;
import br.com.spring.batch.partitioner.storage.BlobPaths;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.stereotype.Service;

@Service
public class BlobPollingService {

    private static final StructuredLogger log = StructuredLogger.of(BlobPollingService.class, "blob-polling");

    private final BlobCatalog catalog;
    private final BlobPaths paths;
    private final OriginalFileRepository repository;

    public BlobPollingService(BlobCatalog catalog, BlobPaths paths, OriginalFileRepository repository) {
        this.catalog = catalog;
        this.paths = paths;
        this.repository = repository;
    }

    public InboxFiles poll() {
        long start = System.currentTimeMillis();
        InboxFiles listed = new InboxFiles(catalog.list(paths.inboxPrefix()).stream()
                .filter(BlobFile::isDataFile)
                .toList());
        List<ReceivedFileDocument> unfinished = repository.findUnfinished();
        InboxFiles inbox = listed.including(unfinished);
        log.info("poll.finish").field("listed", listed.size()).field("unfinished", unfinished.size())
                .field("files", inbox.size()).field("durationMs", System.currentTimeMillis() - start)
                .data("prefix", paths.inboxPrefix()).log("polling do blob concluído");
        return inbox;
    }
}
