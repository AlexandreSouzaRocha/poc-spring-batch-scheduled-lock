package br.com.spring.batch.partitioner.service;

import java.time.Instant;
import java.util.List;

import br.com.spring.batch.partitioner.model.document.BlobLocation;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.storage.BlobFile;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FileIntakeServiceTest {

    @Test
    void includesUnfinishedFilesThatAlreadyLeftTheInbox() {
        BlobFile listed = file("MOV_ABERTO_2026.09.24.001.txt");
        ReceivedFileDocument alreadyListed = registered(listed);
        ReceivedFileDocument movedBeforeCompleting = registered(file("MOV_SALDO_2026.09.24.001.txt"));

        List<BlobFile> pending = FileIntakeService.withUnfinished(List.of(listed),
                List.of(alreadyListed, movedBeforeCompleting));

        assertThat(pending).containsExactly(listed, file("MOV_SALDO_2026.09.24.001.txt"));
        assertThat(pending.stream().map(BlobFile::fileId))
                .containsExactly(alreadyListed.id(), movedBeforeCompleting.id());
    }

    @Test
    void fileIdentityIsTheFileNameOnly() {
        BlobFile original = new BlobFile("entrada/MOV_ABERTO_2026.09.24.001.txt", 100, "etag-1");
        BlobFile sameNameOtherContent = new BlobFile("entrada/MOV_ABERTO_2026.09.24.001.txt", 200, "etag-2");
        BlobFile regenerated = new BlobFile("entrada/MOV_ABERTO_2026.09.24.002.txt", 100, "etag-1");

        assertThat(sameNameOtherContent.fileId()).isEqualTo(original.fileId());
        assertThat(regenerated.fileId()).isNotEqualTo(original.fileId());
    }

    private static ReceivedFileDocument registered(BlobFile file) {
        return ReceivedFileDocument.original(file.fileName(),
                BlobLocation.received(file.path(), file.etag(), file.sizeBytes()), null, Instant.now());
    }

    private static BlobFile file(String name) {
        return new BlobFile("entrada/" + name, 100, "etag-" + name);
    }
}
