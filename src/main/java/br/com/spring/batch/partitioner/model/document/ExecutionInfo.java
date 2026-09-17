package br.com.spring.batch.partitioner.model.document;

import org.springframework.data.mongodb.core.mapping.Field;

public record ExecutionInfo(
        @Field(ReceivedFileFields.JOB_INSTANCE_ID) Long jobInstanceId,
        @Field(ReceivedFileFields.LAST_JOB_EXECUTION_ID) Long lastJobExecutionId,
        @Field(ReceivedFileFields.ATTEMPTS) int attempts,
        @Field(ReceivedFileFields.LAST_ERROR) String lastError,
        @Field(ReceivedFileFields.DURATION_MS) Long durationMs) {

    public static ExecutionInfo notStarted() {
        return new ExecutionInfo(null, null, 0, null, null);
    }

    public static ExecutionInfo written(ExecutionInfo originalExecution, long durationMs) {
        return new ExecutionInfo(originalExecution.jobInstanceId(), originalExecution.lastJobExecutionId(),
                originalExecution.attempts(), null, durationMs);
    }
}
