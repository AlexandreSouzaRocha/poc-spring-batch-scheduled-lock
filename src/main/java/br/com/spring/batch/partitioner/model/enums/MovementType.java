package br.com.spring.batch.partitioner.model.enums;

import java.util.Arrays;
import java.util.Locale;

import br.com.spring.batch.partitioner.model.layout.InvalidFileException;

public enum MovementType {
    ABERTO,
    FECHADO,
    SALDO,
    ULTIMA;

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
