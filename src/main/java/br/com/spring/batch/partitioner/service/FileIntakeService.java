package br.com.spring.batch.partitioner.service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import br.com.spring.batch.partitioner.batch.FileJobExecutions;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.BlobLocation;
import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.MovementFileName;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.storage.BlobFile;
import br.com.spring.batch.partitioner.storage.BlobPaths;
import br.com.spring.batch.partitioner.storage.BlobStorage;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

@Service
public class FileIntakeService {

    private static final StructuredLogger log = StructuredLogger.of(FileIntakeService.class, "file-intake");

    private final OriginalFileRepository repository;
    private final BlobStorage storage;
    private final BlobPaths paths;
    private final FileLayout layout;
    private final PartitionSettings settings;
    private final FileJobExecutions executions;

    public FileIntakeService(OriginalFileRepository repository, BlobStorage storage, BlobPaths paths,
                             FileLayout layout, PartitionSettings settings, FileJobExecutions executions) {
        this.repository = repository;
        this.storage = storage;
        this.paths = paths;
        this.layout = layout;
        this.settings = settings;
        this.executions = executions;
    }

    public List<BlobFile> pendingFiles() {
        long start = System.currentTimeMillis();
        List<BlobFile> listed = storage.list(paths.inboxPrefix()).stream().filter(BlobFile::isDataFile).toList();
        List<ReceivedFileDocument> unfinished = repository.findUnfinished();
        List<BlobFile> pending = withUnfinished(listed, unfinished);
        log.info("poll.finish").field("listed", listed.size()).field("unfinished", unfinished.size())
                .field("files", pending.size()).field("durationMs", System.currentTimeMillis() - start)
                .data("prefix", paths.inboxPrefix()).log("polling do blob concluído");
        return pending;
    }

    public Optional<ReceivedFileDocument> take(BlobFile file) {
        return repository.findById(file.fileId())
                .map(this::resume)
                .orElseGet(() -> register(file));
    }

    private Optional<ReceivedFileDocument> resume(ReceivedFileDocument file) {
        Optional<ReceivedFileDocument> resumed = repository.resume(file.id(), settings.maxAttempts());
        resumed.ifPresentOrElse(partitioning -> logResume(file, partitioning), () -> skip(file));
        return resumed;
    }

    private void skip(ReceivedFileDocument file) {
        if (repository.failExhausted(file.id(), settings.maxAttempts())) {
            log.error("file.fail").field("fileId", file.id()).field("fileName", file.fileName())
                    .field("previousStatus", file.status()).field("attempts", file.attempts())
                    .field("maxAttempts", settings.maxAttempts())
                    .log("tentativas esgotadas; arquivo marcado como FAILED e mantido no blob para tratativa manual");
            return;
        }
        log.debug("file.skip").field("fileId", file.id()).field("fileName", file.fileName())
                .field("status", file.status()).log("arquivo não elegível neste ciclo");
    }

    private Optional<ReceivedFileDocument> register(BlobFile file) {
        if (executions.isCompleted(file.fileName())) {
            log.warn("file.duplicate").field("fileId", file.fileId()).field("fileName", file.fileName())
                    .data("listedPath", file.path()).data("listedEtag", file.etag())
                    .log("arquivo com o mesmo nome já foi processado (JobRepository); ignorado até tratativa manual");
            return Optional.empty();
        }
        ReceivedFileDocument original = ReceivedFileDocument.original(file.fileName(),
                BlobLocation.received(file.path(), file.etag(), file.sizeBytes()), movementOf(file), Instant.now());
        try {
            repository.insert(original);
        } catch (DuplicateKeyException e) {
            log.warn("file.concurrent").field("fileId", original.id()).field("fileName", original.fileName())
                    .error(e).log("arquivo já registrado por outra instância; seguindo para o próximo");
            return Optional.empty();
        }
        log.info("file.register").field("fileId", original.id()).field("fileName", original.fileName())
                .field("sizeBytes", file.sizeBytes()).data("blobPath", file.path()).data("etag", file.etag())
                .log("novo arquivo registrado como PARTITIONING");
        return Optional.of(original);
    }

    private static void logResume(ReceivedFileDocument previous, ReceivedFileDocument resumed) {
        log.warn("file.resume").field("fileId", resumed.id()).field("fileName", resumed.fileName())
                .field("previousStatus", previous.status()).field("attempt", resumed.attempts())
                .log("arquivo não concluído retomado");
    }

    private MovementInfo movementOf(BlobFile file) {
        return MovementFileName.parse(file.fileName())
                .map(MovementInfo::fromFileName)
                .orElseGet(() -> movementFromHeader(file));
    }

    private MovementInfo movementFromHeader(BlobFile file) {
        try {
            return MovementInfo.from(FileHeader.parse(storage.read(file.path(), layout.headerRange()), layout));
        } catch (RuntimeException e) {
            log.warn("file.classify").field("fileId", file.fileId()).field("fileName", file.fileName()).error(e)
                    .log("nome sem data de movimento e header ilegível; a validação do job decide");
            return null;
        }
    }

    static List<BlobFile> withUnfinished(List<BlobFile> listed, List<ReceivedFileDocument> unfinished) {
        Set<String> listedIds = listed.stream().map(BlobFile::fileId).collect(Collectors.toSet());
        Stream<BlobFile> leftTheInbox = unfinished.stream()
                .filter(file -> !listedIds.contains(file.id()))
                .map(file -> new BlobFile(file.blob().sourcePath(), file.sizeBytes(), file.blob().etag()));
        return Stream.concat(listed.stream(), leftTheInbox).toList();
    }
}
