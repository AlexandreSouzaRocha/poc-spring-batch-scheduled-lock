package br.com.spring.batch.partitioner.storage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import br.com.spring.batch.partitioner.storage.azure.AzureBlockUpload;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class AzureBlockUploadTest {

    private final BlockBlobClient blob = mock(BlockBlobClient.class);
    private final ByteArrayOutputStream staged = new ByteArrayOutputStream();

    @Test
    void stagesFixedSizeBlocksAndCommitsInOrder() throws IOException {
        captureStagedBytes();
        byte[] content = "0123456789ABCDEFGHIJ".getBytes();

        try (AzureBlockUpload upload = new AzureBlockUpload(blob, 8)) {
            upload.output().write(content);
            upload.commit();
        }

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> ids = ArgumentCaptor.forClass(List.class);
        verify(blob).commitBlockList(ids.capture(), eq(true));
        assertThat(ids.getValue()).hasSize(3).doesNotHaveDuplicates();
        assertThat(staged.toByteArray()).isEqualTo(content);
    }

    @Test
    void neverCommitsWhenClosedWithoutCommit() throws IOException {
        try (AzureBlockUpload upload = new AzureBlockUpload(blob, 8)) {
            upload.output().write(new byte[20]);
        }

        verify(blob, never()).commitBlockList(any(), eq(true));
    }

    private void captureStagedBytes() {
        doAnswer(invocation -> {
            InputStream data = invocation.getArgument(1);
            staged.write(data.readAllBytes());
            return null;
        }).when(blob).stageBlock(anyString(), any(InputStream.class), anyLong());
    }
}
