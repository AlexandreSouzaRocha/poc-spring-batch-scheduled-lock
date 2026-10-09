package br.com.spring.batch.partitioner.model.layout;

import java.util.Arrays;
import java.util.Optional;

public enum LineType {
    HEADER('H', "header"),
    DETAIL('D', "detalhe"),
    TRAILER('T', "trailer");

    private final byte indicator;
    private final String description;

    LineType(char indicator, String description) {
        this.indicator = (byte) indicator;
        this.description = description;
    }

    public byte indicator() {
        return indicator;
    }

    public String description() {
        return description;
    }

    public boolean matches(byte firstByte) {
        return indicator == firstByte;
    }

    public static Optional<LineType> of(byte firstByte) {
        return Arrays.stream(values()).filter(type -> type.matches(firstByte)).findFirst();
    }

    public static String describe(byte firstByte) {
        return "'" + (char) firstByte + "' (" + of(firstByte).map(LineType::description).orElse("desconhecido") + ")";
    }
}
