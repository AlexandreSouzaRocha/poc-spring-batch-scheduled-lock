package br.com.spring.batch.partitioner.model.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document(collection = ProcessingIndicatorDocument.COLLECTION)
public record ProcessingIndicatorDocument(
        @Id String movementDate,
        @Field(ProcessingIndicatorFields.ABERTO_PROCESSING_DATE) String abertoProcessingDate,
        @Field(ProcessingIndicatorFields.ABERTO_QUANTITY) Long abertoQuantity,
        @Field(ProcessingIndicatorFields.FECHADO_PROCESSING_DATE) String fechadoProcessingDate,
        @Field(ProcessingIndicatorFields.FECHADO_QUANTITY) Long fechadoQuantity,
        @Field(ProcessingIndicatorFields.SALDO_PROCESSING_DATE) String saldoProcessingDate,
        @Field(ProcessingIndicatorFields.SALDO_QUANTITY) Long saldoQuantity,
        @Field(ProcessingIndicatorFields.ULTIMA_PROCESSING_DATE) String ultimaProcessingDate,
        @Field(ProcessingIndicatorFields.ULTIMA_QUANTITY) Long ultimaQuantity) {

    public static final String COLLECTION = "indicador_processamento";
}
