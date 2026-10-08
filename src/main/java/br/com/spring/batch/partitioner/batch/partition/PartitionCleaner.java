package br.com.spring.batch.partitioner.batch.partition;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.PartitionFileRepository;
import br.com.spring.batch.partitioner.storage.BlobStorage;
import br.com.spring.batch.partitioner.storage.BlobPaths;

import org.springframework.stereotype.Component;

@Component
public class PartitionCleaner {

    private final BlobStorage storage;
    private final BlobPaths paths;
    private final PartitionFileRepository repository;

    public PartitionCleaner(BlobStorage storage, BlobPaths paths, PartitionFileRepository repository) {
        this.storage = storage;
        this.paths = paths;
        this.repository = repository;
    }

    public CleanupResult clean(ReceivedFileDocument original) {
        String prefix = paths.partitionPrefix(original);
        int deletedBlobs = storage.deleteByPrefix(prefix);
        long deletedDocuments = repository.deleteByParent(original.id());
        return new CleanupResult(prefix, deletedBlobs, deletedDocuments);
    }

    public record CleanupResult(String prefix, int deletedBlobs, long deletedDocuments) {

        public boolean removedSomething() {
            return deletedBlobs > 0 || deletedDocuments > 0;
        }
    }
}
