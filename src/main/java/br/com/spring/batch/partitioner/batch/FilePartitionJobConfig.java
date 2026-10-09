package br.com.spring.batch.partitioner.batch;

import br.com.spring.batch.partitioner.batch.listener.FileStatusJobListener;
import br.com.spring.batch.partitioner.batch.listener.JobMetricsListener;
import br.com.spring.batch.partitioner.batch.listener.PartitionLinesListener;
import br.com.spring.batch.partitioner.batch.listener.StepMetricsListener;
import br.com.spring.batch.partitioner.batch.partition.FilePartitioner;
import br.com.spring.batch.partitioner.batch.tasklet.CleanupPartitionsTasklet;
import br.com.spring.batch.partitioner.batch.tasklet.FinishFileTasklet;
import br.com.spring.batch.partitioner.batch.tasklet.MoveOriginalTasklet;
import br.com.spring.batch.partitioner.batch.tasklet.PartitionWriterTasklet;
import br.com.spring.batch.partitioner.batch.tasklet.PublishPartitionsTasklet;
import br.com.spring.batch.partitioner.batch.tasklet.RegisterPartitionsTasklet;
import br.com.spring.batch.partitioner.batch.tasklet.ValidateFileTasklet;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;

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

    private static final ResourcelessTransactionManager WITHOUT_MONGO_TRANSACTION = new ResourcelessTransactionManager();

    private final JobRepository jobRepository;
    private final StepMetricsListener stepMetricsListener;

    public FilePartitionJobConfig(JobRepository jobRepository, StepMetricsListener stepMetricsListener) {
        this.jobRepository = jobRepository;
        this.stepMetricsListener = stepMetricsListener;
    }

    @Bean
    public Step validateFileStep(ValidateFileTasklet tasklet) {
        return taskletStep(FilePartitionJob.VALIDATE_FILE_STEP, tasklet, false);
    }

    @Bean
    public Step cleanupPartitionsStep(CleanupPartitionsTasklet tasklet) {
        return taskletStep(FilePartitionJob.CLEANUP_PARTITIONS_STEP, tasklet, true);
    }

    @Bean
    public Step partitionWorkerStep(PartitionWriterTasklet tasklet) {
        return taskletStep(FilePartitionJob.PARTITION_WORKER_STEP, tasklet, false);
    }

    @Bean
    @JobScope
    public FilePartitioner filePartitioner(OriginalFileRepository repository, FileLayout layout,
            @Value("#{jobParameters['" + FilePartitionJob.FILE_NAME_PARAMETER + "']}") String fileName) {
        return new FilePartitioner(repository, ReceivedFileDocument.idOf(fileName), layout);
    }

    @Bean
    public Step partitionMasterStep(Step partitionWorkerStep, FilePartitioner filePartitioner,
                                    TaskExecutor partitionTaskExecutor, PartitionSettings settings,
                                    PartitionLinesListener partitionLinesListener) {
        return new StepBuilder(FilePartitionJob.PARTITION_MASTER_STEP, jobRepository)
                .partitioner(FilePartitionJob.PARTITION_WORKER_STEP, filePartitioner)
                .splitter(allPartitionsOnRestart(filePartitioner))
                .partitionHandler(partitionHandler(partitionWorkerStep, partitionTaskExecutor, settings))
                .listener(stepMetricsListener)
                .listener(partitionLinesListener)
                .build();
    }

    @Bean
    public Step registerPartitionsStep(RegisterPartitionsTasklet tasklet) {
        return taskletStep(FilePartitionJob.REGISTER_PARTITIONS_STEP, tasklet, false);
    }

    @Bean
    public Step publishPartitionsStep(PublishPartitionsTasklet tasklet) {
        return taskletStep(FilePartitionJob.PUBLISH_PARTITIONS_STEP, tasklet, false);
    }

    @Bean
    public Step moveOriginalStep(MoveOriginalTasklet tasklet) {
        return taskletStep(FilePartitionJob.MOVE_ORIGINAL_STEP, tasklet, false);
    }

    @Bean
    public Step finishFileStep(FinishFileTasklet tasklet) {
        return taskletStep(FilePartitionJob.FINISH_FILE_STEP, tasklet, false);
    }

    @Bean
    public Job filePartitionJob(Step validateFileStep, Step cleanupPartitionsStep, Step partitionMasterStep,
                                Step registerPartitionsStep, Step publishPartitionsStep, Step moveOriginalStep,
                                Step finishFileStep,
                                FileStatusJobListener fileStatusJobListener, JobMetricsListener jobMetricsListener) {
        return new JobBuilder(FilePartitionJob.NAME, jobRepository)
                .start(validateFileStep)
                .next(cleanupPartitionsStep)
                .next(partitionMasterStep)
                .next(registerPartitionsStep)
                .next(publishPartitionsStep)
                .next(moveOriginalStep)
                .next(finishFileStep)
                .listener(jobMetricsListener)
                .listener(fileStatusJobListener)
                .build();
    }

    private StepExecutionSplitter allPartitionsOnRestart(FilePartitioner filePartitioner) {
        SimpleStepExecutionSplitter splitter = new SimpleStepExecutionSplitter(jobRepository,
                FilePartitionJob.PARTITION_WORKER_STEP, filePartitioner);
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

    private Step taskletStep(String name, Tasklet tasklet, boolean runOnEveryAttempt) {
        return new StepBuilder(name, jobRepository)
                .tasklet(tasklet, WITHOUT_MONGO_TRANSACTION)
                .allowStartIfComplete(runOnEveryAttempt)
                .listener(stepMetricsListener)
                .build();
    }
}
