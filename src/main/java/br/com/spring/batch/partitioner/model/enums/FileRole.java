package br.com.spring.batch.partitioner.model.enums;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonValue;

public enum FileRole {
    ORIGINAL("ORIGINAL"),
    PARTITION("PARTICAO");

    private final String value;

    FileRole(String value) {
        this.value = value;
    }

    @JsonValue
    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }

    public static FileRole fromValue(String value) {
        return Arrays.stream(values())
                .filter(role -> role.value.equals(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("tipo de arquivo desconhecido: " + value));
    }
}
