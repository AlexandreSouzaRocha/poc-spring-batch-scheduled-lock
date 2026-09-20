package br.com.spring.batch.partitioner.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import br.com.spring.batch.partitioner.model.document.BlobLocation;
import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.queue.MovementDependencies;
import br.com.spring.batch.partitioner.model.queue.ProcessingQueue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProcessingQueueTest {

    private static final int LIMIT = 20;
    private static final MovementDependencies SEM_DEPENDENCIA = MovementDependencies.none();
    private static final MovementDependencies ULTIMA_APOS_ABERTO =
            MovementDependencies.of(Map.of(MovementType.ULTIMA, List.of(MovementType.ABERTO)));

    @Test
    @DisplayName("despacha apenas a data mais antiga, ordenada por tipo dentro dela")
    void dispatchesOldestDateOnly() {
        ProcessingQueue queue = ProcessingQueue.of(List.of(
                file(MovementType.SALDO, "2026-09-18"),
                file(MovementType.FECHADO, "2026-09-19"),
                file(MovementType.ABERTO, "2026-09-18"),
                file(MovementType.FECHADO, "2026-09-18")), List.of(), SEM_DEPENDENCIA, LIMIT);

        assertThat(queue.waveDate()).contains("2026-09-18");
        assertThat(names(queue)).containsExactly(
                "MOV_FECHADO_2026-09-18", "MOV_ABERTO_2026-09-18", "MOV_SALDO_2026-09-18");
    }

    @Test
    @DisplayName("segura o tipo dependente enquanto o pre-requisito da mesma data estiver pendente")
    void holdsDependentTypeWhilePrerequisiteIsPending() {
        ProcessingQueue queue = ProcessingQueue.of(List.of(
                file(MovementType.ULTIMA, "2026-09-18"),
                file(MovementType.ABERTO, "2026-09-18"),
                file(MovementType.FECHADO, "2026-09-18")), List.of(), ULTIMA_APOS_ABERTO, LIMIT);

        assertThat(names(queue)).containsExactly("MOV_FECHADO_2026-09-18", "MOV_ABERTO_2026-09-18");
    }

    @Test
    @DisplayName("libera o tipo dependente quando o pre-requisito da data ja concluiu")
    void releasesDependentTypeWhenPrerequisiteIsDone() {
        ProcessingQueue queue = ProcessingQueue.of(List.of(file(MovementType.ULTIMA, "2026-09-18")),
                List.of(), ULTIMA_APOS_ABERTO, LIMIT);

        assertThat(names(queue)).containsExactly("MOV_ULTIMA_2026-09-18");
    }

    @Test
    @DisplayName("nao inicia a data seguinte enquanto a anterior nao terminar")
    void keepsNextDateWaiting() {
        ProcessingQueue queue = ProcessingQueue.of(List.of(
                file(MovementType.ABERTO, "2026-09-19"),
                file(MovementType.ULTIMA, "2026-09-19"),
                file(MovementType.ULTIMA, "2026-09-18")), List.of(), ULTIMA_APOS_ABERTO, LIMIT);

        assertThat(queue.waveDate()).contains("2026-09-18");
        assertThat(names(queue)).containsExactly("MOV_ULTIMA_2026-09-18");
    }

    @Test
    @DisplayName("bloqueia o que vem depois do arquivo rejeitado na mesma data")
    void blocksEverythingAfterRejectedFile() {
        ReceivedFileDocument rejected = file(MovementType.ABERTO, "2026-09-18");

        ProcessingQueue queue = ProcessingQueue.of(List.of(
                file(MovementType.SALDO, "2026-09-18"),
                file(MovementType.FECHADO, "2026-09-18")), List.of(rejected), SEM_DEPENDENCIA, LIMIT);

        assertThat(names(queue)).containsExactly("MOV_FECHADO_2026-09-18");
        assertThat(queue.blockedBy()).contains(rejected);
    }

    @Test
    @DisplayName("arquivo rejeitado numa data futura nao impede a data corrente")
    void rejectedFileOnLaterDateDoesNotBlockCurrentWave() {
        ReceivedFileDocument rejected = file(MovementType.FECHADO, "2026-09-19");

        ProcessingQueue queue = ProcessingQueue.of(List.of(
                file(MovementType.ABERTO, "2026-09-18"),
                file(MovementType.FECHADO, "2026-09-18")), List.of(rejected), SEM_DEPENDENCIA, LIMIT);

        assertThat(names(queue)).containsExactly("MOV_FECHADO_2026-09-18", "MOV_ABERTO_2026-09-18");
    }

    @Test
    @DisplayName("arquivo sem movimento identificado vai para o inicio e bloqueia todo o resto")
    void unidentifiedFileBlocksQueue() {
        ReceivedFileDocument unidentified = new ReceivedFileDocument("sem-tipo", null, null, "ARQUIVO.txt", null,
                null, null, null, null, null);

        ProcessingQueue queue = ProcessingQueue.of(List.of(file(MovementType.FECHADO, "2026-09-18")),
                List.of(unidentified), SEM_DEPENDENCIA, LIMIT);

        assertThat(queue.isEmpty()).isTrue();
        assertThat(queue.blockedBy()).contains(unidentified);
    }

    @Test
    @DisplayName("respeita o limite do ciclo mantendo a ordem")
    void appliesCycleLimit() {
        ProcessingQueue queue = ProcessingQueue.of(List.of(
                file(MovementType.SALDO, "2026-09-18"),
                file(MovementType.FECHADO, "2026-09-18"),
                file(MovementType.ABERTO, "2026-09-18")), List.of(), SEM_DEPENDENCIA, 2);

        assertThat(names(queue)).containsExactly("MOV_FECHADO_2026-09-18", "MOV_ABERTO_2026-09-18");
    }

    @Test
    @DisplayName("rejeita dependencia ciclica na configuracao")
    void rejectsCyclicDependencies() {
        assertThatThrownBy(() -> MovementDependencies.of(Map.of(
                MovementType.ULTIMA, List.of(MovementType.ABERTO),
                MovementType.ABERTO, List.of(MovementType.ULTIMA))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cíclica");
    }

    @Test
    @DisplayName("agrupa por tipo preservando a ordem da fila e a ordem dentro do grupo")
    void groupsByMovementTypeKeepingOrder() {
        ProcessingQueue queue = ProcessingQueue.of(List.of(
                file(MovementType.ABERTO, "2026-09-19"),
                file(MovementType.FECHADO, "2026-09-19"),
                file(MovementType.ABERTO, "2026-09-18"),
                file(MovementType.FECHADO, "2026-09-18")), List.of(), SEM_DEPENDENCIA, LIMIT);

        assertThat(queue.byMovementGroup().keySet()).containsExactly("fechado", "aberto");
        assertThat(queue.byMovementGroup().get("fechado"))
                .extracting(ReceivedFileDocument::fileName)
                .containsExactly("MOV_FECHADO_2026-09-18");
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
