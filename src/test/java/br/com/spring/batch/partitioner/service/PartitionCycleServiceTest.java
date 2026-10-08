package br.com.spring.batch.partitioner.service;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import br.com.spring.batch.partitioner.storage.BlobFile;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionCycleServiceTest {

    @Test
    void groupsFilesByMovementTypeFromFileName() {
        BlobFile abertoHoje = file("MOV_ABERTO_2026.09.24.001.txt");
        BlobFile fechado = file("MOV_FECHADO_2026.09.24.001.txt");
        BlobFile abertoOntem = file("MOV_ABERTO_2026.09.23.001.txt");
        BlobFile semPadrao = file("arquivo-livre.txt");

        Map<String, List<BlobFile>> groups = PartitionCycleService.byMovementGroup(
                List.of(abertoHoje, fechado, abertoOntem, semPadrao));

        assertThat(groups).containsOnlyKeys("aberto", "fechado", PartitionCycleService.UNKNOWN_GROUP);
        assertThat(groups.get("aberto")).containsExactly(abertoHoje, abertoOntem);
        assertThat(groups.get("fechado")).containsExactly(fechado);
        assertThat(groups.get(PartitionCycleService.UNKNOWN_GROUP)).containsExactly(semPadrao);
    }

    @Test
    void keepsEveryListedFileWithoutLimit() {
        List<BlobFile> files = IntStream.range(0, 150)
                .mapToObj(index -> file("MOV_SALDO_2026.09.24." + index + ".txt"))
                .toList();

        assertThat(PartitionCycleService.byMovementGroup(files).get("saldo")).hasSize(150);
    }

    private static BlobFile file(String name) {
        return new BlobFile("entrada/" + name, 100, "etag-" + name);
    }
}
