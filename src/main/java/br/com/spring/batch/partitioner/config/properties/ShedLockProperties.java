package br.com.spring.batch.partitioner.config.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app.shedlock")
public record ShedLockProperties(
        @NotBlank String collection,
        @NotBlank String lockedBy,
        @Valid @NotNull FieldNames fields) {

    public record FieldNames(
            @NotBlank String name,
            @NotBlank String lockUntil,
            @NotBlank String lockedAt,
            @NotBlank String lockedBy) {

        private static final String MONGO_ID = "_id";

        public boolean nameIsMongoId() {
            return MONGO_ID.equals(name);
        }
    }
}
