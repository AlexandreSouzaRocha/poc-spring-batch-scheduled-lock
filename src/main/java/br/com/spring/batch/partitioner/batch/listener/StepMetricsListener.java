package br.com.spring.batch.partitioner.batch.listener;

import br.com.spring.batch.partitioner.batch.job.BatchNames;
import br.com.spring.batch.partitioner.batch.job.FileJobParameters;
import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.batch.metrics.Throughput;
import br.com.spring.batch.partitioner.support.log.ErrorSummary;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.event.Level;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

@Component
public class StepMetricsListener implements StepExecutionListener {

    private static final StructuredLogger log = StructuredLogger.of(StepMetricsListener.class, "metrics");
    private static final String STEP_NAME_SEPARATOR = ":";

    private final MeterRegistry meterRegistry;

    public StepMetricsListener(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        Throughput throughput = Throughput.between(stepExecution.getStartTime(), stepExecution.getEndTime(),
                volumeOf(stepExecution));
        record(stepExecution, throughput);
        logMetrics(stepExecution, throughput);
        return stepExecution.getExitStatus();
    }

    private static StepVolume volumeOf(StepExecution stepExecution) {
        if (!BatchNames.PARTITION_MASTER_STEP.equals(stepExecution.getStepName())) {
            return StepVolume.readFrom(stepExecution.getExecutionContext());
        }
        return stepExecution.getJobExecution().getStepExecutions().stream()
                .filter(worker -> BatchNames.isPartitionWorker(worker.getStepName()))
                .map(worker -> StepVolume.readFrom(worker.getExecutionContext()))
                .reduce(StepVolume.empty(), StepVolume::plus);
    }

    private void record(StepExecution stepExecution, Throughput throughput) {
        Timer.builder("partitioner.step.duration")
                .description("Duração dos steps do filePartitionJob")
                .tag("step", baseName(stepExecution.getStepName()))
                .tag("status", stepExecution.getStatus().name())
                .register(meterRegistry)
                .record(throughput.duration());
    }

    private static void logMetrics(StepExecution stepExecution, Throughput throughput) {
        log.at(stepExecution.getStatus().isUnsuccessful() ? Level.WARN : Level.INFO, "step.metrics")
                .field("step", stepExecution.getStepName())
                .field("fileId", FileJobParameters.fileIdOf(stepExecution))
                .field("jobExecutionId", stepExecution.getJobExecutionId())
                .field("status", stepExecution.getStatus())
                .field("durationMs", throughput.durationMs())
                .field("lines", throughput.volume().lines())
                .field("bytes", throughput.volume().bytes())
                .field("linesPerSec", throughput.linesPerSecond())
                .field("mbPerSec", throughput.megabytesPerSecond())
                .data("stepExecutionId", stepExecution.getId())
                .data("failures", stepExecution.getFailureExceptions().stream().map(ErrorSummary::of).toList())
                .log("STEP_METRICS");
    }

    private static String baseName(String stepName) {
        return stepName.split(STEP_NAME_SEPARATOR)[0];
    }
}
