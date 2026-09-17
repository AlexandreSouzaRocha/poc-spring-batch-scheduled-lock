package br.com.spring.batch.partitioner.batch.listener;

import java.util.Optional;

import br.com.spring.batch.partitioner.batch.job.FileJobParameters;
import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.batch.metrics.Throughput;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.log.ErrorSummary;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.event.Level;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class JobMetricsListener implements JobExecutionListener {

    private static final StructuredLogger log = StructuredLogger.of(JobMetricsListener.class, "metrics");

    private final MeterRegistry meterRegistry;
    private final OriginalFileRepository repository;

    public JobMetricsListener(MeterRegistry meterRegistry, OriginalFileRepository repository) {
        this.meterRegistry = meterRegistry;
        this.repository = repository;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        String fileId = FileJobParameters.fileIdOf(jobExecution);
        Optional<ReceivedFileDocument> original = repository.findById(fileId).filter(ReceivedFileDocument::hasMovement);
        StepVolume volume = original.map(file -> new StepVolume(file.lineCount(), file.sizeBytes()))
                .orElse(StepVolume.empty());
        Throughput throughput = Throughput.between(jobExecution.getStartTime(), jobExecution.getEndTime(), volume);
        record(jobExecution, throughput);
        logMetrics(jobExecution, fileId, throughput, original);
    }

    private void record(JobExecution jobExecution, Throughput throughput) {
        Timer.builder("partitioner.job.duration")
                .description("Duração total do filePartitionJob por arquivo")
                .tag("status", jobExecution.getStatus().name())
                .register(meterRegistry)
                .record(throughput.duration());
    }

    private static void logMetrics(JobExecution jobExecution, String fileId, Throughput throughput,
                                   Optional<ReceivedFileDocument> original) {
        boolean completed = jobExecution.getStatus() == BatchStatus.COMPLETED;
        log.at(completed ? Level.INFO : Level.WARN, "job.metrics")
                .field("job", jobExecution.getJobInstance().getJobName())
                .field("fileId", fileId)
                .field("jobExecutionId", jobExecution.getId())
                .field("status", jobExecution.getStatus())
                .field("attempt", original.map(ReceivedFileDocument::attempts).orElse(0))
                .field("durationMs", throughput.durationMs())
                .field("lines", throughput.volume().lines())
                .field("bytes", throughput.volume().bytes())
                .field("partitions", original.map(ReceivedFileDocument::partitionCount).orElse(0))
                .field("linesPerSec", throughput.linesPerSecond())
                .field("mbPerSec", throughput.megabytesPerSecond())
                .data("fileName", original.map(ReceivedFileDocument::fileName).orElse(null))
                .data("failures", jobExecution.getAllFailureExceptions().stream().map(ErrorSummary::of).toList())
                .log("JOB_METRICS");
    }
}
