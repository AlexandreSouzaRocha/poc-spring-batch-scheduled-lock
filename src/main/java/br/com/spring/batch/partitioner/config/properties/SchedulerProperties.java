package br.com.spring.batch.partitioner.config.properties;

import java.time.Duration;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app.scheduler.file-processing")
public record SchedulerProperties(
        @NotBlank String lockName,
        @NotNull Duration lockAtMostFor,
        @NotNull Duration lockAtLeastFor) {

    private static final String SEPARATOR = "-";
    private static final String POLL_SUFFIX = "poll";

    public String lockName() {
        return lockName;
    }

    public String pollLockName() {
        return lockName + SEPARATOR + POLL_SUFFIX;
    }

    public String lockNameFor(String movementGroup) {
        return lockName + SEPARATOR + movementGroup;
    }
}
