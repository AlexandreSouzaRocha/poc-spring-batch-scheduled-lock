package br.com.spring.batch.partitioner.service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.FileTrailer;
import br.com.spring.batch.partitioner.service.FileQueryService.FileView;
import br.com.spring.batch.partitioner.storage.BlobFile;
import br.com.spring.batch.partitioner.storage.BlobPaths;
import br.com.spring.batch.partitioner.storage.BlobStorage;

import org.springframework.stereotype.Service;

@Service
public class FileVerificationService {

    private final FileQueryService queryService;
    private final BlobStorage storage;
    private final BlobPaths paths;
    private final FileLayout layout;
    private final PartitionSettings settings;

    public FileVerificationService(FileQueryService queryService, BlobStorage storage, BlobPaths paths,
                                   FileLayout layout, PartitionSettings settings) {
        this.queryService = queryService;
        this.storage = storage;
        this.paths = paths;
        this.layout = layout;
        this.settings = settings;
    }

    public Optional<VerificationReport> verify(String fileId) {
        return queryService.find(fileId).map(this::verify);
    }

    private VerificationReport verify(FileView file) {
        List<ReceivedFileDocument> documents = queryService.partitionsOf(file.id());
        Map<String, BlobFile> partitionBlobs = partitionBlobsOf(file);
        List<PartitionCheck> checks = documents.stream()
                .map(partition -> check(file, partition, partitionBlobs.get(partition.currentPath())))
                .toList();
        return new VerificationReport(file.id(), file.status(), file.originalRemoved(), file.currentPath(),
                storage.exists(file.currentPath()), storage.exists(file.sourcePath()), partitionBlobs.size(),
                documents.size(), checks.stream().mapToLong(PartitionCheck::lines).sum(),
                checks.stream().allMatch(PartitionCheck::valid), checks);
    }

    private Map<String, BlobFile> partitionBlobsOf(FileView file) {
        if (file.movement() == null) {
            return Map.of();
        }
        String prefix = paths.partitionPrefix(file.movement().type(), file.movement().date(), file.id());
        return storage.list(prefix).stream().collect(Collectors.toMap(BlobFile::path, Function.identity()));
    }

    private PartitionCheck check(FileView file, ReceivedFileDocument partition, BlobFile blob) {
        long expectedSize = layout.headerLineBytes() + partition.lineCount() * layout.detailLineBytes()
                + (settings.writeTrailer() ? layout.trailerLineBytes() : 0);
        boolean exists = blob != null;
        boolean sizeMatches = exists && blob.sizeBytes() == expectedSize;
        boolean headerMatches = exists && Arrays.equals(storage.read(blob.path(), layout.headerRange()),
                file.movement().headerLineBytes(layout));
        Boolean trailerMatches = settings.writeTrailer() ? exists && trailerMatches(partition, blob) : null;
        return new PartitionCheck(partition.currentPath(), exists, sizeMatches, headerMatches, trailerMatches,
                partition.audit().publishedAt() != null, partition.lineCount());
    }

    private boolean trailerMatches(ReceivedFileDocument partition, BlobFile blob) {
        String expected = FileTrailer.of(partition.lineCount(), LocalDate.parse(partition.trailer().processingDate()),
                layout).text();
        byte[] actual = storage.read(blob.path(), layout.trailerRange(partition.lineCount()));
        return expected.equals(new String(actual, StandardCharsets.US_ASCII));
    }

    public record VerificationReport(String fileId, String status, boolean originalRemoved, String currentPath,
                                     boolean currentPathExists, boolean sourcePathExists, int partitionBlobs,
                                     int partitionDocuments, long partitionLines, boolean allPartitionsValid,
                                     List<PartitionCheck> partitions) {
    }

    public record PartitionCheck(String path, boolean exists, boolean sizeMatches, boolean headerMatches,
                                 Boolean trailerMatches, boolean published, long lines) {

        boolean valid() {
            return exists && sizeMatches && headerMatches && !Boolean.FALSE.equals(trailerMatches) && published;
        }
    }
}
