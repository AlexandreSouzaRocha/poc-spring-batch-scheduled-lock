package br.com.spring.batch.partitioner.support;

import java.time.Instant;
import java.util.Optional;

import br.com.spring.batch.partitioner.batch.FilePartitionJob;
import br.com.spring.batch.partitioner.model.document.BlobLocation;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.support.chaos.ChaosAction;
import br.com.spring.batch.partitioner.support.chaos.ChaosPoint;
import br.com.spring.batch.partitioner.support.chaos.ChaosRule;
import br.com.spring.batch.partitioner.support.chaos.ChaosTaskletInterceptor;
import br.com.spring.batch.partitioner.support.chaos.FailureInjector;
import br.com.spring.batch.partitioner.support.chaos.SimulatedFailureException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.ObjectProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChaosTaskletInterceptorTest {

    private static final String FILE_NAME = "MOV_ABERTO_2026.10.09.001.txt";

    private final FailureInjector injector = new FailureInjector();
    private ChaosTaskletInterceptor interceptor;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        OriginalFileRepository originals = mock(OriginalFileRepository.class);
        when(originals.findById(anyString())).thenReturn(Optional.of(ReceivedFileDocument.original(FILE_NAME,
                BlobLocation.received("entrada/" + FILE_NAME, "etag", 100), null, Instant.now())));
        ObjectProvider<FailureInjector> injectorProvider = mock(ObjectProvider.class);
        when(injectorProvider.getObject()).thenReturn(injector);
        ObjectProvider<OriginalFileRepository> originalsProvider = mock(ObjectProvider.class);
        when(originalsProvider.getObject()).thenReturn(originals);
        interceptor = new ChaosTaskletInterceptor(injectorProvider, originalsProvider);
    }

    @Test
    void failsBeforeTheStepRunsAtStepPoints() {
        RecordingTasklet tasklet = new RecordingTasklet();
        injector.activate(new ChaosRule(ChaosPoint.MOVE, ChaosAction.FAIL, 1, null, 0));

        Tasklet proxied = proxy(tasklet);

        assertThatThrownBy(() -> proxied.execute(contribution(FilePartitionJob.MOVE_ORIGINAL_STEP, null), null))
                .isInstanceOf(SimulatedFailureException.class);
        assertThat(tasklet.executions).isZero();
    }

    @Test
    void failsAfterThePartitionWasWritten() {
        RecordingTasklet tasklet = new RecordingTasklet();
        injector.activate(new ChaosRule(ChaosPoint.PARTITION, ChaosAction.FAIL, 1, 3, 0));
        ExecutionContext partition3 = new PartitionRange(3, 0, 10, new ByteRange(0, 1510)).toExecutionContext();

        Tasklet proxied = proxy(tasklet);

        assertThatThrownBy(() -> proxied.execute(contribution(
                FilePartitionJob.PARTITION_WORKER_STEP + ":partition0003", partition3), null))
                .isInstanceOf(SimulatedFailureException.class);
        assertThat(tasklet.executions).isEqualTo(1);
    }

    @Test
    void runsTheStepNormallyWithoutAMatchingRule() throws Exception {
        RecordingTasklet tasklet = new RecordingTasklet();
        injector.activate(new ChaosRule(ChaosPoint.PUBLISH, ChaosAction.FAIL, 1, null, 0));

        assertThat(proxy(tasklet).execute(contribution(FilePartitionJob.VALIDATE_FILE_STEP, null), null))
                .isEqualTo(RepeatStatus.FINISHED);
        assertThat(tasklet.executions).isEqualTo(1);
    }

    @Test
    void leavesBeansThatAreNotTaskletsUntouched() {
        Object bean = new Object();

        assertThat(interceptor.postProcessAfterInitialization(bean, "bean")).isSameAs(bean);
    }

    private Tasklet proxy(Tasklet tasklet) {
        return (Tasklet) interceptor.postProcessAfterInitialization(tasklet, "tasklet");
    }

    private static StepContribution contribution(String stepName, ExecutionContext context) {
        JobExecution jobExecution = mock(JobExecution.class);
        when(jobExecution.getJobParameters()).thenReturn(FilePartitionJob.parametersFor(FILE_NAME));
        StepExecution stepExecution = mock(StepExecution.class);
        when(stepExecution.getStepName()).thenReturn(stepName);
        when(stepExecution.getJobExecution()).thenReturn(jobExecution);
        when(stepExecution.getExecutionContext()).thenReturn(context);
        StepContribution contribution = mock(StepContribution.class);
        when(contribution.getStepExecution()).thenReturn(stepExecution);
        return contribution;
    }

    public static class RecordingTasklet implements Tasklet {

        private int executions;

        @Override
        public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
            executions++;
            return RepeatStatus.FINISHED;
        }
    }
}
