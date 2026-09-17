package br.com.spring.batch.partitioner.storage.azure;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import br.com.spring.batch.partitioner.storage.BlobUpload;
import com.azure.storage.blob.specialized.BlockBlobClient;

public class AzureBlockUpload extends OutputStream implements BlobUpload {

    private static final String BLOCK_ID_FORMAT = "%010d";

    private final StagedBlocks blocks;
    private final byte[] buffer;
    private int position;

    public AzureBlockUpload(BlockBlobClient blob, int blockSizeBytes) {
        this.blocks = new StagedBlocks(blob, new ArrayList<>());
        this.buffer = new byte[blockSizeBytes];
    }

    @Override
    public OutputStream output() {
        return this;
    }

    @Override
    public void write(int value) {
        buffer[position++] = (byte) value;
        stageIfFull();
    }

    @Override
    public void write(byte[] bytes, int offset, int length) {
        int written = 0;
        while (written < length) {
            int chunk = Math.min(length - written, buffer.length - position);
            System.arraycopy(bytes, offset + written, buffer, position, chunk);
            position += chunk;
            written += chunk;
            stageIfFull();
        }
    }

    @Override
    public void commit() {
        stagePending();
        blocks.commit();
    }

    @Override
    public void close() {
        position = 0;
    }

    private void stageIfFull() {
        if (position == buffer.length) {
            stagePending();
        }
    }

    private void stagePending() {
        if (position == 0) {
            return;
        }
        blocks.stage(buffer, position);
        position = 0;
    }

    private record StagedBlocks(BlockBlobClient blob, List<String> ids) {

        void stage(byte[] data, int length) {
            String id = Base64.getEncoder().encodeToString(
                    String.format(BLOCK_ID_FORMAT, ids.size()).getBytes(StandardCharsets.US_ASCII));
            blob.stageBlock(id, new ByteArrayInputStream(data, 0, length), length);
            ids.add(id);
        }

        void commit() {
            blob.commitBlockList(ids, true);
        }
    }
}
