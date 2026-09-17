package br.com.spring.batch.partitioner.storage;

import java.time.Instant;

import br.com.spring.batch.partitioner.config.properties.AppProperties.FolderSettings;
import br.com.spring.batch.partitioner.model.document.BlobLocation;
import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.enums.MovementType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BlobPathsTest {

    private final BlobPaths paths = new BlobPaths(new FolderSettings("entrada", "processados", "erros"));

    @Test
    void buildsFolderLayoutForEveryDestination() {
        ReceivedFileDocument original = inspectedOriginal();

        assertThat(paths.inboxPath("MOV.dat")).isEqualTo("entrada/MOV.dat");
        assertThat(paths.partitionPrefix(original)).isEqualTo("ultima/2026-09-16/file-1/");
        assertThat(paths.partitionPath(original, 7)).isEqualTo("ultima/2026-09-16/file-1/MOV_part_0007.dat");
        assertThat(paths.processedPath(original)).isEqualTo("processados/2026-09-16/file-1/MOV.dat");
        assertThat(paths.errorPath(original)).isEqualTo("erros/file-1/MOV.dat");
        assertThat(BlobPaths.fileNameOf("a/b/c.dat")).isEqualTo("c.dat");
    }

    private static ReceivedFileDocument inspectedOriginal() {
        ReceivedFileDocument received = ReceivedFileDocument.original("file-1", "MOV.dat",
                BlobLocation.received("entrada/MOV.dat", "etag", 100), Instant.now());
        return new ReceivedFileDocument(received.id(), received.role(), null, received.fileName(), received.status(),
                received.blob(), new MovementInfo("H2026-09-16ULTIMA ", MovementType.ULTIMA, "2026-09-16"), null,
                received.execution(), received.audit());
    }
}
