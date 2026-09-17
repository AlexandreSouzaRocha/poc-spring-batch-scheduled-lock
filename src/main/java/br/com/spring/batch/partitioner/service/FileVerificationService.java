package br.com.spring.batch.partitioner.service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.repository.PartitionFileRepository;
import br.com.spring.batch.partitioner.storage.BlobCatalog;
import br.com.spring.batch.partitioner.storage.BlobFile;
import br.com.spring.batch.partitioner.storage.BlobPaths;
import br.com.spring.batch.partitioner.storage.BlobReader;

import org.springframework.stereotype.Service;

@Service
public class FileVerificationService {

    private static final ByteRange HEADER_RANGE = new ByteRange(0, FileLayout.HEADER_LINE_BYTES);

    private final OriginalFileRepository originals;
    private final PartitionFileRepository partitions;
    private final BlobInspection blobs;

    public FileVerificationService(OriginalFileRepository originals, PartitionFileRepository partitions,
                                   BlobCatalog catalog, BlobReader reader, BlobPaths paths) {
        this.originals = originals;
        this.partitions = partitions;
        this.blobs = new BlobInspection(catalog, reader, paths);
    }

    public Optional<VerificationReport> verify(String fileId) {
        return originals.findById(fileId).map(this::verify);
    }

    private VerificationReport verify(ReceivedFileDocument original) {
        List<ReceivedFileDocument> documents = partitions.findByParent(original.id());
        Map<String, BlobFile> partitionBlobs = blobs.partitionBlobs(original);
        List<PartitionCheck> checks = documents.stream()
                .map(partition -> blobs.check(original, partition, partitionBlobs.get(partition.currentPath())))
                .toList();
        return new VerificationReport(original.id(), original.status().name(), original.currentPath(),
                blobs.exists(original.currentPath()), blobs.exists(original.blob().sourcePath()),
                partitionBlobs.size(), documents.size(), checks.stream().mapToLong(PartitionCheck::lines).sum(),
                checks.stream().allMatch(PartitionCheck::valid), checks);
    }

    public record VerificationReport(String fileId, String status, String currentPath, boolean currentPathExists,
                                     boolean sourcePathExists, int partitionBlobs, int partitionDocuments,
                                     long partitionLines, boolean allPartitionsValid, List<PartitionCheck> partitions) {
    }

    public record PartitionCheck(String path, boolean exists, boolean sizeMatches, boolean headerMatches,
                                 boolean published, long lines) {

        boolean valid() {
            return exists && sizeMatches && headerMatches && published;
        }
    }

    private record BlobInspection(BlobCatalog catalog, BlobReader reader, BlobPaths paths) {

        Map<String, BlobFile> partitionBlobs(ReceivedFileDocument original) {
            if (!original.hasMovement()) {
                return Map.of();
            }
            return catalog.list(paths.partitionPrefix(original)).stream()
                    .collect(Collectors.toMap(BlobFile::path, Function.identity()));
        }

        boolean exists(String path) {
            return catalog.list(path).stream().anyMatch(file -> file.path().equals(path));
        }

        PartitionCheck check(ReceivedFileDocument original, ReceivedFileDocument partition, BlobFile blob) {
            long expectedSize = FileLayout.HEADER_LINE_BYTES + partition.lineCount() * FileLayout.RECORD_LINE_BYTES;
            boolean exists = blob != null;
            boolean sizeMatches = exists && blob.sizeBytes() == expectedSize;
            boolean headerMatches = exists && Arrays.equals(reader.read(blob.path(), HEADER_RANGE),
                    original.headerLineBytes());
            return new PartitionCheck(partition.currentPath(), exists, sizeMatches, headerMatches,
                    partition.audit().publishedAt() != null, partition.lineCount());
        }
    }
}
