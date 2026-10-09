package br.com.spring.batch.partitioner.model.document;

import br.com.spring.batch.partitioner.model.layout.FileTrailer;

import org.springframework.data.mongodb.core.mapping.Field;

public record TrailerInfo(
        @Field(ReceivedFileFields.RECORD_COUNT) long recordCount,
        @Field(ReceivedFileFields.PROCESSING_DATE) String processingDate) {

    public static TrailerInfo from(FileTrailer trailer) {
        return new TrailerInfo(trailer.recordCount(), trailer.processingDateText());
    }
}
