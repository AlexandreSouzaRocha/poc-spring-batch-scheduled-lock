package br.com.spring.batch.partitioner.batch.partition;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.PartitionFileRepository;
import br.com.spring.batch.partitioner.storage.BlobCatalog;
import br.com.spring.batch.partitioner.storage.BlobPaths;

import org.springframework.stereotype.Component;

@Component
public class PartitionCleaner {

    private final BlobCatalog catalog;
    private final BlobPaths paths;
    private final PartitionFileRepository repository;

    public PartitionCleaner(BlobCatalog catalog, BlobPaths paths, PartitionFileRepository repository) {
        this.catalog = catalog;
        this.paths = paths;
        this.repository = repository;
    }

    public CleanupResult clean(ReceivedFileDocument original) {
        String prefix = paths.partitionPrefix(original);
        int deletedBlobs = catalog.deleteByPrefix(prefix);
        long deletedDocuments = repository.deleteByParent(original.id());
        return new CleanupResult(prefix, deletedBlobs, deletedDocuments, false);
    }

    public CleanupResult cleanIfUnpublished(ReceivedFileDocument original) {
        if (!original.hasMovement() || repository.anyPublished(original.id())) {
            return CleanupResult.notApplicable();
        }
        return clean(original);
    }

    public record CleanupResult(String prefix, int deletedBlobs, long deletedDocuments, boolean skipped) {

        static CleanupResult notApplicable() {
            return new CleanupResult(null, 0, 0, true);
        }

        public boolean removedSomething() {
            return deletedBlobs > 0 || deletedDocuments > 0;
        }
    }
}
