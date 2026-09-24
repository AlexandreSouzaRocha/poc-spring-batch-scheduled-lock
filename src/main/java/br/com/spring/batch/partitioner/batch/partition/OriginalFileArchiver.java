package br.com.spring.batch.partitioner.batch.partition;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.storage.BlobMover;
import br.com.spring.batch.partitioner.storage.BlobPaths;

import org.springframework.stereotype.Component;

@Component
public class OriginalFileArchiver {

    private final BlobMover mover;
    private final BlobPaths paths;
    private final OriginalFileRepository repository;

    public OriginalFileArchiver(BlobMover mover, BlobPaths paths, OriginalFileRepository repository) {
        this.mover = mover;
        this.paths = paths;
        this.repository = repository;
    }

    public String moveToProcessed(ReceivedFileDocument original) {
        String target = paths.processedPath(original);
        if (target.equals(original.currentPath())) {
            return target;
        }
        mover.move(original.currentPath(), target);
        repository.relocate(original.id(), target);
        return target;
    }
}
