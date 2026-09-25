package br.com.spring.batch.partitioner.service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import br.com.spring.batch.partitioner.model.document.BlobLocation;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.storage.BlobFile;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InboxFilesTest {

    @Test
    void groupsFilesByMovementTypeFromFileName() {
        BlobFile abertoHoje = file("MOV_ABERTO_2026.09.24.001.txt");
        BlobFile fechado = file("MOV_FECHADO_2026.09.24.001.txt");
        BlobFile abertoOntem = file("MOV_ABERTO_2026.09.23.001.txt");
        BlobFile semPadrao = file("arquivo-livre.txt");

        Map<String, List<BlobFile>> groups = new InboxFiles(List.of(abertoHoje, fechado, abertoOntem, semPadrao))
                .byMovementGroup();

        assertThat(groups).containsOnlyKeys("aberto", "fechado", InboxFiles.UNKNOWN_GROUP);
        assertThat(groups.get("aberto")).containsExactly(abertoHoje, abertoOntem);
        assertThat(groups.get("fechado")).containsExactly(fechado);
        assertThat(groups.get(InboxFiles.UNKNOWN_GROUP)).containsExactly(semPadrao);
    }

    @Test
    void keepsEveryListedFileWithoutLimit() {
        List<BlobFile> files = IntStream.range(0, 150)
                .mapToObj(index -> file("MOV_SALDO_2026.09.24." + index + ".txt"))
                .toList();

        InboxFiles inbox = new InboxFiles(files);

        assertThat(inbox.size()).isEqualTo(150);
        assertThat(inbox.byMovementGroup().get("saldo")).hasSize(150);
    }

    @Test
    void includesUnfinishedFilesThatAlreadyLeftTheInbox() {
        BlobFile listed = file("MOV_ABERTO_2026.09.24.001.txt");
        ReceivedFileDocument alreadyListed = registered(listed);
        ReceivedFileDocument movedBeforeCompleting = registered(file("MOV_SALDO_2026.09.24.001.txt"));

        InboxFiles inbox = new InboxFiles(List.of(listed)).including(List.of(alreadyListed, movedBeforeCompleting));

        assertThat(inbox.files()).containsExactly(listed, file("MOV_SALDO_2026.09.24.001.txt"));
        assertThat(inbox.files().stream().map(FileIntakeService::idOf))
                .containsExactly(alreadyListed.id(), movedBeforeCompleting.id());
    }

    @Test
    void fileIdentityIsTheFileNameOnly() {
        BlobFile original = new BlobFile("entrada/MOV_ABERTO_2026.09.24.001.txt", 100, "etag-1");
        BlobFile sameNameOtherContent = new BlobFile("entrada/MOV_ABERTO_2026.09.24.001.txt", 200, "etag-2");
        BlobFile regenerated = new BlobFile("entrada/MOV_ABERTO_2026.09.24.002.txt", 100, "etag-1");

        assertThat(FileIntakeService.idOf(sameNameOtherContent)).isEqualTo(FileIntakeService.idOf(original));
        assertThat(FileIntakeService.idOf(regenerated)).isNotEqualTo(FileIntakeService.idOf(original));
    }

    private static ReceivedFileDocument registered(BlobFile file) {
        return ReceivedFileDocument.original(file.fileName(),
                BlobLocation.received(file.path(), file.etag(), file.sizeBytes()), null, Instant.now());
    }

    private static BlobFile file(String name) {
        return new BlobFile("entrada/" + name, 100, "etag-" + name);
    }
}
