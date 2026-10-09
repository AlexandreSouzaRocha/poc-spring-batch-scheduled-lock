package br.com.spring.batch.partitioner.service;

import java.util.List;
import java.util.Optional;

import br.com.spring.batch.partitioner.batch.FileJobExecutions;
import br.com.spring.batch.partitioner.batch.FileJobExecutions.ExecutionSummary;
import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.enums.FileStatus;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.repository.PartitionFileRepository;
import br.com.spring.batch.partitioner.storage.BlobPaths;

import org.springframework.stereotype.Service;

@Service
public class FileQueryService {

    private final OriginalFileRepository originals;
    private final PartitionFileRepository partitions;
    private final FileJobExecutions executions;
    private final BlobPaths paths;

    public FileQueryService(OriginalFileRepository originals, PartitionFileRepository partitions,
                            FileJobExecutions executions, BlobPaths paths) {
        this.originals = originals;
        this.partitions = partitions;
        this.executions = executions;
        this.paths = paths;
    }

    public Optional<FileDetail> detail(String fileId) {
        List<ReceivedFileDocument> partitionDocuments = partitionsOf(fileId);
        return find(fileId, partitionDocuments).map(file -> new FileDetail(file.id(), file.fileName(), file.status(),
                file.attempts(), file.originalRemoved(), file.original(),
                executions.summary(file.fileName()).orElse(null), partitionDocuments));
    }

    public Optional<FileView> find(String fileId) {
        return find(fileId, partitionsOf(fileId));
    }

    public List<ReceivedFileDocument> partitionsOf(String fileId) {
        return partitions.findByParentFileIdOrderByPartitioningIndexAsc(fileId);
    }

    private Optional<FileView> find(String fileId, List<ReceivedFileDocument> partitionDocuments) {
        Optional<ReceivedFileDocument> original = originals.findById(fileId);
        if (original.isPresent()) {
            return original.map(FileView::of);
        }
        return partitionDocuments.stream().findFirst().map(partition -> removedOriginal(fileId, partition));
    }

    private FileView removedOriginal(String fileId, ReceivedFileDocument partition) {
        String sourcePath = partition.blob().sourcePath();
        String fileName = BlobPaths.fileNameOf(sourcePath);
        return new FileView(fileId, fileName, FileStatus.COMPLETED.value(), executions.attempts(fileName), sourcePath,
                paths.processedPath(partition.movementDate(), fileId, fileName), partition.movement(), null);
    }

    public record FileView(String id, String fileName, String status, Integer attempts, String sourcePath,
                           String currentPath, MovementInfo movement, ReceivedFileDocument original) {

        static FileView of(ReceivedFileDocument original) {
            return new FileView(original.id(), original.fileName(), original.status().value(), original.attempts(),
                    original.blob().sourcePath(), original.currentPath(), original.movement(), original);
        }

        public boolean originalRemoved() {
            return original == null;
        }
    }

    public record FileDetail(String fileId, String fileName, String status, Integer attempts,
                             boolean originalRemoved, ReceivedFileDocument original, ExecutionSummary lastExecution,
                             List<ReceivedFileDocument> partitions) {
    }
}
