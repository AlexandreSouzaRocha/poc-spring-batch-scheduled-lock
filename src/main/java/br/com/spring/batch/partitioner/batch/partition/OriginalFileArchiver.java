package br.com.spring.batch.partitioner.batch.partition;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.storage.BlobPaths;
import br.com.spring.batch.partitioner.storage.BlobStorage;

import org.springframework.stereotype.Component;

@Component
public class OriginalFileArchiver {

    private final BlobStorage storage;
    private final BlobPaths paths;
    private final OriginalFileRepository repository;

    public OriginalFileArchiver(BlobStorage storage, BlobPaths paths, OriginalFileRepository repository) {
        this.storage = storage;
        this.paths = paths;
        this.repository = repository;
    }

    public String moveToProcessed(ReceivedFileDocument original) {
        String target = paths.processedPath(original);
        if (target.equals(original.currentPath())) {
            return target;
        }
        storage.move(original.currentPath(), target);
        repository.relocate(original.id(), target);
        return target;
    }
}
