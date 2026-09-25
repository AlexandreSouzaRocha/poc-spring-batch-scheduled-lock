package br.com.spring.batch.partitioner.service;

import java.time.Instant;
import java.util.Optional;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.BlobLocation;
import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.enums.FileStatus;
import br.com.spring.batch.partitioner.model.layout.MovementFileName;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.storage.BlobFile;
import br.com.spring.batch.partitioner.storage.BlobHeaderReader;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

@Service
public class FileIntakeService {

    private static final StructuredLogger log = StructuredLogger.of(FileIntakeService.class, "file-intake");

    private final OriginalFileRepository repository;
    private final BlobHeaderReader headerReader;
    private final PartitionSettings settings;

    public FileIntakeService(OriginalFileRepository repository, BlobHeaderReader headerReader,
                             PartitionSettings settings) {
        this.repository = repository;
        this.headerReader = headerReader;
        this.settings = settings;
    }

    public Optional<ReceivedFileDocument> take(BlobFile file) {
        return repository.findById(idOf(file))
                .map(existing -> resume(existing, file))
                .orElseGet(() -> register(file));
    }

    private Optional<ReceivedFileDocument> resume(ReceivedFileDocument file, BlobFile listed) {
        Optional<ReceivedFileDocument> resumed = repository.resume(file.id(), settings.maxAttempts());
        resumed.ifPresentOrElse(partitioning -> logResume(file, partitioning), () -> skip(file, listed));
        return resumed;
    }

    private void skip(ReceivedFileDocument file, BlobFile listed) {
        if (file.status() == FileStatus.COMPLETED) {
            log.warn("file.duplicate").field("fileId", file.id()).field("fileName", file.fileName())
                    .data("listedPath", listed.path()).data("listedEtag", listed.etag())
                    .data("processedPath", file.currentPath())
                    .log("arquivo com o mesmo nome já foi processado; ignorado até tratativa manual");
            return;
        }
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
        ReceivedFileDocument original = ReceivedFileDocument.original(file.fileName(),
                BlobLocation.received(file.path(), file.etag(), file.sizeBytes()), movementOf(file), Instant.now());
        try {
            repository.register(original);
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
            return MovementInfo.from(headerReader.read(file.path()));
        } catch (RuntimeException e) {
            log.warn("file.classify").field("fileId", idOf(file)).field("fileName", file.fileName()).error(e)
                    .log("nome sem data de movimento e header ilegível; a validação do job decide");
            return null;
        }
    }

    static String idOf(BlobFile file) {
        return ReceivedFileDocument.idOf(file.fileName());
    }
}
