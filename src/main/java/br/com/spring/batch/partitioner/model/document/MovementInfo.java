package br.com.spring.batch.partitioner.model.document;

import java.nio.charset.StandardCharsets;

import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;

import org.springframework.data.mongodb.core.mapping.Field;

public record MovementInfo(
        @Field(ReceivedFileFields.HEADER) String header,
        @Field(ReceivedFileFields.TYPE) MovementType type,
        @Field(ReceivedFileFields.DATE) String date) {

    public static MovementInfo from(FileHeader fileHeader) {
        return new MovementInfo(fileHeader.text(), fileHeader.movementType(), fileHeader.movementDateText());
    }

    public byte[] headerLineBytes() {
        return (header + (char) FileLayout.LINE_SEPARATOR).getBytes(StandardCharsets.US_ASCII);
    }
}
