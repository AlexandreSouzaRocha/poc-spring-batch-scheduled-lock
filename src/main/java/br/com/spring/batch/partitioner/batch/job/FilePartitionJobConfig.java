package br.com.spring.batch.partitioner.batch.job;

import br.com.spring.batch.partitioner.batch.listener.FileStatusJobListener;
import br.com.spring.batch.partitioner.batch.listener.JobMetricsListener;
import br.com.spring.batch.partitioner.batch.listener.StepMetricsListener;
import br.com.spring.batch.partitioner.batch.partition.FilePartitioner;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.batch.step.FileStepSupport;
import br.com.spring.batch.partitioner.batch.tasklet.CleanupPartitionsTasklet;
import br.com.spring.batch.partitioner.batch.tasklet.MoveOriginalTasklet;
import br.com.spring.batch.partitioner.batch.tasklet.PartitionWriterTasklet;
import br.com.spring.batch.partitioner.batch.tasklet.PublishPartitionsTasklet;
import br.com.spring.batch.partitioner.batch.tasklet.RegisterPartitionsTasklet;
import br.com.spring.batch.partitioner.batch.tasklet.ValidateHeaderTasklet;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;

import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.StepExecutionSplitter;
import org.springframework.batch.core.partition.support.SimpleStepExecutionSplitter;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;

@Configuration
public class FilePartitionJobConfig {

    private final JobRepository jobRepository;
    private final StepFactory stepFactory;

    public FilePartitionJobConfig(JobRepository jobRepository, StepMetricsListener stepMetricsListener) {
        this.jobRepository = jobRepository;
        this.stepFactory = new StepFactory(jobRepository, stepMetricsListener);
    }

    @Bean
    public Step validateHeaderStep(ValidateHeaderTasklet tasklet) {
        return stepFactory.tasklet(BatchNames.VALIDATE_HEADER_STEP, tasklet, false);
    }

    @Bean
    public Step cleanupPartitionsStep(CleanupPartitionsTasklet tasklet) {
        return stepFactory.tasklet(BatchNames.CLEANUP_PARTITIONS_STEP, tasklet, true);
    }

    @Bean
    public Step partitionWorkerStep(PartitionWriterTasklet tasklet) {
        return stepFactory.tasklet(BatchNames.PARTITION_WORKER_STEP, tasklet, false);
    }

    @Bean
    @JobScope
    public FilePartitioner filePartitioner(FileStepSupport support, FileLayout layout,
                                           @Value("#{jobParameters['" + FileJobParameters.FILE_ID + "']}") String fileId) {
        return new FilePartitioner(support, fileId, layout);
    }

    @Bean
    public Step partitionMasterStep(Step partitionWorkerStep, FilePartitioner filePartitioner,
                                    TaskExecutor partitionTaskExecutor, PartitionSettings settings) {
        return new StepBuilder(BatchNames.PARTITION_MASTER_STEP, jobRepository)
                .partitioner(BatchNames.PARTITION_WORKER_STEP, filePartitioner)
                .splitter(allPartitionsOnRestart(filePartitioner))
                .partitionHandler(partitionHandler(partitionWorkerStep, partitionTaskExecutor, settings))
                .listener(stepFactory.metricsListener())
                .build();
    }

    @Bean
    public Step registerPartitionsStep(RegisterPartitionsTasklet tasklet) {
        return stepFactory.tasklet(BatchNames.REGISTER_PARTITIONS_STEP, tasklet, false);
    }

    @Bean
    public Step moveOriginalStep(MoveOriginalTasklet tasklet) {
        return stepFactory.tasklet(BatchNames.MOVE_ORIGINAL_STEP, tasklet, false);
    }

    @Bean
    public Step publishPartitionsStep(PublishPartitionsTasklet tasklet) {
        return stepFactory.tasklet(BatchNames.PUBLISH_PARTITIONS_STEP, tasklet, false);
    }

    @Bean
    public Job filePartitionJob(Step validateHeaderStep, Step cleanupPartitionsStep, Step partitionMasterStep,
                                Step registerPartitionsStep, Step moveOriginalStep, Step publishPartitionsStep,
                                FileStatusJobListener fileStatusJobListener, JobMetricsListener jobMetricsListener) {
        return new JobBuilder(BatchNames.JOB_NAME, jobRepository)
                .start(validateHeaderStep)
                .next(cleanupPartitionsStep)
                .next(partitionMasterStep)
                .next(registerPartitionsStep)
                .next(moveOriginalStep)
                .next(publishPartitionsStep)
                .listener(fileStatusJobListener)
                .listener(jobMetricsListener)
                .build();
    }

    private StepExecutionSplitter allPartitionsOnRestart(FilePartitioner filePartitioner) {
        SimpleStepExecutionSplitter splitter = new SimpleStepExecutionSplitter(jobRepository,
                BatchNames.PARTITION_WORKER_STEP, filePartitioner);
        splitter.setAllowStartIfComplete(true);
        return splitter;
    }

    private static TaskExecutorPartitionHandler partitionHandler(Step workerStep, TaskExecutor taskExecutor,
                                                                 PartitionSettings settings) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(workerStep);
        handler.setTaskExecutor(taskExecutor);
        handler.setGridSize(settings.count());
        return handler;
    }

    private record StepFactory(JobRepository jobRepository, StepMetricsListener metricsListener) {

        private static final ResourcelessTransactionManager WITHOUT_MONGO_TRANSACTION =
                new ResourcelessTransactionManager();

        Step tasklet(String name, Tasklet tasklet, boolean runOnEveryAttempt) {
            return new StepBuilder(name, jobRepository)
                    .tasklet(tasklet, WITHOUT_MONGO_TRANSACTION)
                    .allowStartIfComplete(runOnEveryAttempt)
                    .listener(metricsListener)
                    .build();
        }
    }
}
