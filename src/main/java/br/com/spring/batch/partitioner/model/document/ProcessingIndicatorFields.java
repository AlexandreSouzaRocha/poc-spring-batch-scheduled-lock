package br.com.spring.batch.partitioner.model.document;

import br.com.spring.batch.partitioner.model.enums.MovementType;

public final class ProcessingIndicatorFields {

    public static final String PROCESSING_DATE_PREFIX = "data_processamento_movimento_";
    public static final String QUANTITY_PREFIX = "quantidade_movimento_";

    public static final String ABERTO_PROCESSING_DATE = PROCESSING_DATE_PREFIX + "aberto";
    public static final String ABERTO_QUANTITY = QUANTITY_PREFIX + "aberto";
    public static final String FECHADO_PROCESSING_DATE = PROCESSING_DATE_PREFIX + "fechado";
    public static final String FECHADO_QUANTITY = QUANTITY_PREFIX + "fechado";
    public static final String SALDO_PROCESSING_DATE = PROCESSING_DATE_PREFIX + "saldo";
    public static final String SALDO_QUANTITY = QUANTITY_PREFIX + "saldo";
    public static final String ULTIMA_PROCESSING_DATE = PROCESSING_DATE_PREFIX + "ultima";
    public static final String ULTIMA_QUANTITY = QUANTITY_PREFIX + "ultima";

    private ProcessingIndicatorFields() {
    }

    public static String processingDateOf(MovementType type) {
        return PROCESSING_DATE_PREFIX + type.folder();
    }

    public static String quantityOf(MovementType type) {
        return QUANTITY_PREFIX + type.folder();
    }
}
