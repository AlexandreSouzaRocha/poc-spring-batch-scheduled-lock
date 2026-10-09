package br.com.spring.batch.partitioner.model.enums;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonValue;

public enum FileStatus {
    PARTITIONING("PARTICIONANDO"),
    FAILED_PARTITIONING("FALHA_PARTICIONAMENTO"),
    FAILED("FALHA"),
    UPLOADED("ENVIADO"),
    COMPLETED("CONCLUIDO");

    public static final List<FileStatus> UNFINISHED = List.of(PARTITIONING, FAILED_PARTITIONING);

    private final String value;

    FileStatus(String value) {
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

    public static FileStatus fromValue(String value) {
        return Arrays.stream(values())
                .filter(status -> status.value.equals(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("status de arquivo desconhecido: " + value));
    }
}
