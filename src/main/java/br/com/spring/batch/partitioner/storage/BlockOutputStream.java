package br.com.spring.batch.partitioner.storage;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import com.azure.storage.blob.specialized.BlockBlobClient;

public class BlockOutputStream extends OutputStream {

    private final BlockBlobClient blob;
    private final List<String> stagedIds = new ArrayList<>();
    private final byte[] buffer;
    private int position;

    public BlockOutputStream(BlockBlobClient blob, int blockSizeBytes) {
        this.blob = blob;
        this.buffer = new byte[blockSizeBytes];
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

    public void commit() {
        stagePending();
        blob.commitBlockList(stagedIds, true);
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
        String id = BlockList.blockId(stagedIds.size());
        blob.stageBlock(id, new ByteArrayInputStream(buffer, 0, position), position);
        stagedIds.add(id);
        position = 0;
    }
}
