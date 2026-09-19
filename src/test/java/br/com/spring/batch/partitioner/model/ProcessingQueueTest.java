package br.com.spring.batch.partitioner.model;

import java.time.Instant;
import java.util.List;

import br.com.spring.batch.partitioner.model.document.BlobLocation;
import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.queue.ProcessingQueue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessingQueueTest {

    private static final int LIMIT = 20;

    @Test
    @DisplayName("ordena por tipo e depois por data, do mais antigo para o mais novo")
    void ordersByTypeThenDate() {
        ProcessingQueue queue = ProcessingQueue.of(List.of(
                file(MovementType.SALDO, "2026-09-18"),
                file(MovementType.FECHADO, "2026-09-19"),
                file(MovementType.ABERTO, "2026-09-18"),
                file(MovementType.FECHADO, "2026-09-18")), List.of(), LIMIT);

        assertThat(names(queue)).containsExactly(
                "MOV_FECHADO_2026-09-18", "MOV_FECHADO_2026-09-19",
                "MOV_ABERTO_2026-09-18", "MOV_SALDO_2026-09-18");
    }

    @Test
    @DisplayName("bloqueia tudo que vem depois do arquivo rejeitado")
    void blocksEverythingAfterRejectedFile() {
        ReceivedFileDocument rejected = file(MovementType.FECHADO, "2026-09-19");

        ProcessingQueue queue = ProcessingQueue.of(List.of(
                file(MovementType.ABERTO, "2026-09-18"),
                file(MovementType.SALDO, "2026-09-18"),
                file(MovementType.FECHADO, "2026-09-18")), List.of(rejected), LIMIT);

        assertThat(names(queue)).containsExactly("MOV_FECHADO_2026-09-18");
        assertThat(queue.blockedBy()).contains(rejected);
    }

    @Test
    @DisplayName("arquivo sem movimento identificado vai para o inicio e bloqueia todo o resto")
    void unidentifiedFileBlocksQueue() {
        ReceivedFileDocument unidentified = new ReceivedFileDocument("sem-tipo", null, null, "ARQUIVO.txt", null,
                null, null, null, null, null);

        ProcessingQueue queue = ProcessingQueue.of(List.of(file(MovementType.FECHADO, "2026-09-18")),
                List.of(unidentified), LIMIT);

        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.blockedBy()).contains(unidentified);
    }

    @Test
    @DisplayName("respeita o limite do ciclo mantendo a ordem")
    void appliesCycleLimit() {
        ProcessingQueue queue = ProcessingQueue.of(List.of(
                file(MovementType.SALDO, "2026-09-18"),
                file(MovementType.FECHADO, "2026-09-18"),
                file(MovementType.ABERTO, "2026-09-18")), List.of(), 2);

        assertThat(names(queue)).containsExactly("MOV_FECHADO_2026-09-18", "MOV_ABERTO_2026-09-18");
    }

    private static List<String> names(ProcessingQueue queue) {
        return queue.files().stream().map(ReceivedFileDocument::fileName).toList();
    }

    private static ReceivedFileDocument file(MovementType type, String date) {
        return ReceivedFileDocument.original("id-" + type + date, "MOV_" + type + "_" + date,
                BlobLocation.received("entrada/arquivo.txt", "etag", 1000L),
                new MovementInfo(null, type, date), Instant.now());
    }
}
