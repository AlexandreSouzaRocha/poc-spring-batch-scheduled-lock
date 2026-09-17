package br.com.spring.batch.partitioner.batch.partition;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.partition.PartitionPlan;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import br.com.spring.batch.partitioner.repository.PartitionFileRepository;
import br.com.spring.batch.partitioner.storage.BlobCatalog;
import br.com.spring.batch.partitioner.storage.BlobFile;
import br.com.spring.batch.partitioner.storage.BlobPaths;

import org.springframework.stereotype.Component;

@Component
public class PartitionRegistration {

    private final BlobCatalog catalog;
    private final BlobPaths paths;
    private final PartitionFileRepository repository;

    public PartitionRegistration(BlobCatalog catalog, BlobPaths paths, PartitionFileRepository repository) {
        this.catalog = catalog;
        this.paths = paths;
        this.repository = repository;
    }

    public List<ReceivedFileDocument> registerUploaded(ReceivedFileDocument original) {
        Map<String, BlobFile> uploaded = catalog.list(paths.partitionPrefix(original)).stream()
                .collect(Collectors.toMap(BlobFile::path, Function.identity()));
        Instant now = Instant.now();
        List<ReceivedFileDocument> partitions = PartitionPlan.split(original.lineCount(), original.partitionCount())
                .ranges().stream()
                .map(range -> toDocument(original, range, uploaded, now))
                .toList();
        repository.replaceAll(original.id(), partitions);
        return partitions;
    }

    private ReceivedFileDocument toDocument(ReceivedFileDocument original, PartitionRange range,
                                            Map<String, BlobFile> uploaded, Instant now) {
        String path = paths.partitionPath(original, range.index());
        requireUploaded(path, range, Optional.ofNullable(uploaded.get(path)));
        return original.uploadedPartition(range, paths.partitionFileName(original, range.index()), path, now);
    }

    private static void requireUploaded(String path, PartitionRange range, Optional<BlobFile> blob) {
        boolean complete = blob.map(file -> file.sizeBytes() == range.fileSizeBytes()).orElse(false);
        if (!complete) {
            throw new IllegalStateException("partição " + path + " ausente ou incompleta no blob; esperado "
                    + range.fileSizeBytes() + " bytes, encontrado " + blob.map(BlobFile::sizeBytes).orElse(0L));
        }
    }
}
