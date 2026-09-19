package br.com.spring.batch.partitioner.service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import br.com.spring.batch.partitioner.model.document.BlobLocation;
import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.model.layout.MovementFileName;
import br.com.spring.batch.partitioner.storage.BlobCatalog;
import br.com.spring.batch.partitioner.storage.BlobHeaderReader;
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
    private final BlobHeaderReader headerReader;

    public BlobPollingService(BlobCatalog catalog, BlobPaths paths, OriginalFileRepository repository,
                              BlobHeaderReader headerReader) {
        this.catalog = catalog;
        this.paths = paths;
        this.repository = repository;
        this.headerReader = headerReader;
    }

    public long poll() {
        long start = System.currentTimeMillis();
        List<BlobFile> dataFiles = catalog.list(paths.inboxPrefix()).stream().filter(BlobFile::isDataFile).toList();
        long registered = dataFiles.stream().filter(this::register).count();
        log.info("poll.finish").field("found", dataFiles.size()).field("registered", registered)
                .field("durationMs", System.currentTimeMillis() - start).data("prefix", paths.inboxPrefix())
                .log("polling do blob concluído");
        return registered;
    }

    private boolean register(BlobFile file) {
        ReceivedFileDocument original = ReceivedFileDocument.original(idOf(file), file.fileName(),
                BlobLocation.received(file.path(), file.etag(), file.sizeBytes()), movementOf(file), Instant.now());
        boolean registered = repository.register(original);
        logRegistration(original, file, registered);
        return registered;
    }

    private MovementInfo movementOf(BlobFile file) {
        return MovementFileName.parse(file.fileName())
                .map(MovementInfo::fromFileName)
                .orElseGet(() -> movementFromHeader(file));
    }

    private MovementInfo movementFromHeader(BlobFile file) {
        try {
            return MovementInfo.from(headerReader.read(file.path()));
        } catch (RuntimeException e) {
            log.warn("file.classify").field("fileName", file.fileName()).error(e)
                    .log("nome sem data de movimento e header ilegível; arquivo entra no início da fila");
            return null;
        }
    }

    private static void logRegistration(ReceivedFileDocument original, BlobFile file, boolean registered) {
        if (!registered) {
            return;
        }
        log.info("file.register").field("fileId", original.id()).field("fileName", original.fileName())
                .field("sizeBytes", file.sizeBytes()).data("blobPath", file.path()).data("etag", file.etag())
                .log("novo arquivo registrado para particionamento");
    }

    static String idOf(BlobFile file) {
        byte[] identity = (file.path() + "|" + file.etag()).getBytes(StandardCharsets.UTF_8);
        return UUID.nameUUIDFromBytes(identity).toString();
    }
}
