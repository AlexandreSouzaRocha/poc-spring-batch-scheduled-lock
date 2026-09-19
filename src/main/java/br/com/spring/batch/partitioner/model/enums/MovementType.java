package br.com.spring.batch.partitioner.model.enums;

import java.util.Arrays;
import java.util.Locale;

import br.com.spring.batch.partitioner.model.layout.InvalidFileException;

public enum MovementType {
    FECHADO(1),
    ABERTO(2),
    ULTIMA(3),
    SALDO(4);

    private final int processingOrder;

    MovementType(int processingOrder) {
        this.processingOrder = processingOrder;
    }

    public int processingOrder() {
        return processingOrder;
    }

    public String folder() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static MovementType fromHeaderField(String rawValue) {
        String value = rawValue.strip();
        return Arrays.stream(values())
                .filter(type -> type.name().equals(value))
                .findFirst()
                .orElseThrow(() -> new InvalidFileException("tipo de movimento inválido no header: '" + rawValue + "'"));
    }
}
