package br.com.spring.batch.partitioner.model;

import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.layout.MovementFileName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class MovementFileNameTest {

    @Test
    @DisplayName("extrai tipo e data do nome vindo do mainframe")
    void parsesMainframeName() {
        MovementFileName name = MovementFileName.parse("MOV_FECHADO_2026.09.19.10.30.00.123456.txt").orElseThrow();

        assertThat(name.type()).isEqualTo(MovementType.FECHADO);
        assertThat(name.movementDate()).isEqualTo("2026-09-19");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "MOV_DESCONHECIDO_2026.09.19.10.30.00.123456.txt",
            "MOV_ABERTO_20260919_1789760383204_01.txt",
            "MOV_ABERTO_2026.09.19.txt",
            "ARQUIVO_ABERTO_2026.09.19.10.30.00.123456.txt",
            "MOV_ABERTO_2026.09.19.10.30.00.123456.dat"
    })
    @DisplayName("nao extrai quando o nome foge do padrao ou o tipo e desconhecido")
    void rejectsUnknownNames(String fileName) {
        assertThat(MovementFileName.parse(fileName)).isEmpty();
    }
}
