package br.com.spring.batch.partitioner.model.document;

import java.time.Instant;

import org.springframework.data.mongodb.core.mapping.Field;

public record AuditInfo(
        @Field(ReceivedFileFields.CREATED_AT) Instant createdAt,
        @Field(ReceivedFileFields.UPDATED_AT) Instant updatedAt,
        @Field(ReceivedFileFields.PUBLISHED_AT) Instant publishedAt) {

    public static AuditInfo createdAt(Instant now) {
        return new AuditInfo(now, now, null);
    }
}
