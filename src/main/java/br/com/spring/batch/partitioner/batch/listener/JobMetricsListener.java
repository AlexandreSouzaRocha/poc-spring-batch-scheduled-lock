package br.com.spring.batch.partitioner.batch.listener;

import br.com.spring.batch.partitioner.batch.FileJobExecutions;
import br.com.spring.batch.partitioner.batch.FilePartitionJob;
import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.batch.metrics.Throughput;
import br.com.spring.batch.partitioner.support.log.ErrorSummary;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.event.Level;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.stereotype.Component;

@Component
public class JobMetricsListener implements JobExecutionListener {

    private static final StructuredLogger log = StructuredLogger.of(JobMetricsListener.class, "metrics");

    private final MeterRegistry meterRegistry;
    private final FileJobExecutions executions;

    public JobMetricsListener(MeterRegistry meterRegistry, FileJobExecutions executions) {
        this.meterRegistry = meterRegistry;
        this.executions = executions;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        ExecutionContext summary = jobExecution.getExecutionContext();
        StepVolume volume = new StepVolume(summary.getLong(FilePartitionJob.CONTEXT_DETAIL_LINES, 0L),
                summary.getLong(FilePartitionJob.CONTEXT_FILE_SIZE_BYTES, 0L));
        Throughput throughput = Throughput.between(jobExecution.getStartTime(), jobExecution.getEndTime(), volume);
        record(jobExecution, throughput);
        logMetrics(jobExecution, summary, throughput);
    }

    private void record(JobExecution jobExecution, Throughput throughput) {
        Timer.builder("partitioner.job.duration")
                .description("Duração total do filePartitionJob por arquivo")
                .tag("status", jobExecution.getStatus().name())
                .register(meterRegistry)
                .record(throughput.duration());
    }

    private void logMetrics(JobExecution jobExecution, ExecutionContext summary, Throughput throughput) {
        boolean completed = jobExecution.getStatus() == BatchStatus.COMPLETED;
        log.at(completed ? Level.INFO : Level.WARN, "job.metrics")
                .field("job", jobExecution.getJobInstance().getJobName())
                .field("fileId", FilePartitionJob.fileIdOf(jobExecution))
                .field("jobExecutionId", jobExecution.getId())
                .field("status", jobExecution.getStatus())
                .field("attempt", executions.executionCount(jobExecution.getJobInstance()))
                .field("durationMs", throughput.durationMs())
                .field("lines", throughput.volume().lines())
                .field("linesRead", summary.getLong(FilePartitionJob.CONTEXT_DETAIL_LINES_READ, 0L))
                .field("trailerRecordCount", summary.getLong(FilePartitionJob.CONTEXT_TRAILER_RECORD_COUNT, 0L))
                .field("bytes", throughput.volume().bytes())
                .field("partitions", summary.getInt(FilePartitionJob.CONTEXT_PARTITIONS, 0))
                .field("linesPerSec", throughput.linesPerSecond())
                .field("mbPerSec", throughput.megabytesPerSecond())
                .data("fileName", FilePartitionJob.fileNameOf(jobExecution))
                .data("failures", jobExecution.getAllFailureExceptions().stream().map(ErrorSummary::of).toList())
                .log("JOB_METRICS");
    }
}
