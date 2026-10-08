package br.com.spring.batch.partitioner.storage;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.IntStream;

import br.com.spring.batch.partitioner.model.partition.ByteRange;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.specialized.BlockBlobClient;

public class BlockList {

    private static final String BLOCK_ID_FORMAT = "%010d";

    private final BlockBlobClient blob;

    BlockList(BlockBlobClient blob) {
        this.blob = blob;
    }

    public void stage(int blockIndex, byte[] data, int length) {
        blob.stageBlock(blockId(blockIndex), new ByteArrayInputStream(data, 0, length), length);
    }

    public void stageFromUrl(int blockIndex, String sourceUrl, ByteRange sourceRange) {
        blob.stageBlockFromUrl(blockId(blockIndex), sourceUrl,
                new BlobRange(sourceRange.start(), sourceRange.length()));
    }

    public void commit(int blockCount) {
        List<String> orderedIds = IntStream.range(0, blockCount).mapToObj(BlockList::blockId).toList();
        blob.commitBlockList(orderedIds, true);
    }

    static String blockId(int blockIndex) {
        return Base64.getEncoder().encodeToString(
                BLOCK_ID_FORMAT.formatted(blockIndex).getBytes(StandardCharsets.US_ASCII));
    }
}
