package br.com.spring.batch.partitioner.service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

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
public class FileClaimService {

    private static final StructuredLogger log = StructuredLogger.of(FileClaimService.class, "file-claim");

    private final OriginalFileRepository repository;
    private final BlobHeaderReader headerReader;
    private final PartitionSettings settings;

    public FileClaimService(OriginalFileRepository repository, BlobHeaderReader headerReader,
                            PartitionSettings settings) {
        this.repository = repository;
        this.headerReader = headerReader;
        this.settings = settings;
    }

    public Optional<ReceivedFileDocument> claim(BlobFile file) {
        return repository.findById(idOf(file))
                .map(existing -> claimExisting(existing, file))
                .orElseGet(() -> register(file));
    }

    private Optional<ReceivedFileDocument> claimExisting(ReceivedFileDocument file, BlobFile listed) {
        Instant staleBefore = Instant.now().minus(settings.staleAfter());
        Optional<ReceivedFileDocument> claimed = repository.claimForReprocessing(file.id(), staleBefore,
                settings.maxAttempts());
        claimed.ifPresentOrElse(reprocessing -> logReprocessing(file, reprocessing),
                () -> skip(file, listed, staleBefore));
        return claimed;
    }

    private void skip(ReceivedFileDocument file, BlobFile listed, Instant staleBefore) {
        if (file.status() == FileStatus.COMPLETED) {
            log.warn("file.duplicate").field("fileId", file.id()).field("fileName", file.fileName())
                    .data("listedPath", listed.path()).data("listedEtag", listed.etag())
                    .data("processedPath", file.currentPath()).data("completedAt", file.audit().completedAt())
                    .log("arquivo com o mesmo nome já foi processado; ignorado até tratativa manual");
            return;
        }
        skipUnfinished(file, staleBefore);
    }

    private void skipUnfinished(ReceivedFileDocument file, Instant staleBefore) {
        String reason = "instância caiu durante o processamento e as tentativas se esgotaram ("
                + file.attempts() + "/" + settings.maxAttempts() + ")";
        if (repository.failAbandoned(file.id(), staleBefore, settings.maxAttempts(), reason)) {
            log.error("file.fail").field("fileId", file.id()).field("fileName", file.fileName())
                    .field("previousStatus", file.status()).field("attempts", file.attempts())
                    .data("reason", reason).log("arquivo marcado como FAILED; aguardando tratativa manual");
            return;
        }
        log.debug("file.skip").field("fileId", file.id()).field("fileName", file.fileName())
                .field("status", file.status()).data("updatedAt", file.audit().updatedAt())
                .log("arquivo não elegível neste ciclo");
    }

    private Optional<ReceivedFileDocument> register(BlobFile file) {
        ReceivedFileDocument original = ReceivedFileDocument.original(idOf(file), file.fileName(),
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

    private static void logReprocessing(ReceivedFileDocument previous, ReceivedFileDocument claimed) {
        log.warn("file.reprocess").field("fileId", claimed.id()).field("fileName", claimed.fileName())
                .field("previousStatus", previous.status()).field("attempt", claimed.attempts())
                .data("updatedAt", previous.audit().updatedAt()).data("lastError", previous.lastError())
                .log("arquivo assumido para reprocessamento");
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
        return UUID.nameUUIDFromBytes(file.fileName().getBytes(StandardCharsets.UTF_8)).toString();
    }
}
