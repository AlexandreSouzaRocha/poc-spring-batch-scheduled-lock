package br.com.spring.batch.partitioner.batch.partition;

import java.util.LinkedHashMap;
import java.util.Map;

import br.com.spring.batch.partitioner.batch.step.FileStepSupport;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.partition.PartitionPlan;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;

public class FilePartitioner implements Partitioner {

    private static final StructuredLogger log = StructuredLogger.of(FilePartitioner.class, "partition-job");

    private final FileStepSupport support;
    private final String fileId;

    public FilePartitioner(FileStepSupport support, String fileId) {
        this.support = support;
        this.fileId = fileId;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        ReceivedFileDocument original = support.load(fileId);
        PartitionPlan plan = PartitionPlan.split(original.lineCount(), original.partitionCount());
        Map<String, ExecutionContext> contexts = new LinkedHashMap<>();
        plan.ranges().forEach(range -> contexts.put(range.stepName(), range.toExecutionContext()));
        log.info("partition.plan").field("fileId", fileId).field("partitions", plan.size())
                .field("lines", plan.totalLines()).log("plano de particionamento gerado");
        return contexts;
    }
}
