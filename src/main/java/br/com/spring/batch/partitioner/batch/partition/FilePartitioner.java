package br.com.spring.batch.partitioner.batch.partition;

import java.util.LinkedHashMap;
import java.util.Map;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.partition.PartitionPlan;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;

public class FilePartitioner implements Partitioner {

    private static final StructuredLogger log = StructuredLogger.of(FilePartitioner.class, "partition-job");

    private final OriginalFileRepository repository;
    private final String fileId;
    private final FileLayout layout;

    public FilePartitioner(OriginalFileRepository repository, String fileId, FileLayout layout) {
        this.repository = repository;
        this.fileId = fileId;
        this.layout = layout;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        ReceivedFileDocument original = repository.getById(fileId);
        PartitionPlan plan = PartitionPlan.split(original.lineCount(), original.partitionCount(), layout);
        Map<String, ExecutionContext> contexts = new LinkedHashMap<>();
        plan.ranges().forEach(range -> contexts.put(range.stepName(), range.toExecutionContext()));
        log.info("partition.plan").field("fileId", fileId).field("partitions", plan.size())
                .field("lines", plan.totalLines()).log("plano de particionamento gerado");
        return contexts;
    }
}
