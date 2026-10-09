package br.com.spring.batch.partitioner.batch.listener;

import java.util.List;

import br.com.spring.batch.partitioner.batch.FilePartitionJob;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.stereotype.Component;

@Component
public class PartitionLinesListener implements StepExecutionListener {

    private static final StructuredLogger log = StructuredLogger.of(PartitionLinesListener.class, "partition-job");

    @Override
    public ExitStatus afterStep(StepExecution masterStep) {
        if (masterStep.getStatus() != BatchStatus.COMPLETED) {
            return masterStep.getExitStatus();
        }
        List<ExecutionContext> workers = masterStep.getJobExecution().getStepExecutions().stream()
                .filter(step -> FilePartitionJob.isPartitionWorker(step.getStepName()))
                .map(StepExecution::getExecutionContext)
                .toList();
        long linesRead = workers.stream()
                .mapToLong(worker -> worker.getLong(FilePartitionJob.CONTEXT_PARTITION_LINES_READ, 0L))
                .sum();
        boolean verified = workers.stream().allMatch(worker ->
                Boolean.TRUE.equals(worker.get(FilePartitionJob.CONTEXT_PARTITION_LINES_VERIFIED)));
        ExecutionContext job = masterStep.getJobExecution().getExecutionContext();
        job.putLong(FilePartitionJob.CONTEXT_DETAIL_LINES_READ, linesRead);
        job.put(FilePartitionJob.CONTEXT_LINES_VERIFIED, verified);
        log.info("partition.lines").field("fileId", FilePartitionJob.fileIdOf(masterStep))
                .field("partitions", workers.size()).field("linesRead", linesRead)
                .field("detailLines", job.getLong(FilePartitionJob.CONTEXT_DETAIL_LINES, 0L))
                .field("trailerRecordCount", job.getLong(FilePartitionJob.CONTEXT_TRAILER_RECORD_COUNT, 0L))
                .field("lineCheck", verified ? "verified" : "skipped")
                .log("linhas de detalhe lidas no particionamento, sem header e trailer");
        return masterStep.getExitStatus();
    }
}
