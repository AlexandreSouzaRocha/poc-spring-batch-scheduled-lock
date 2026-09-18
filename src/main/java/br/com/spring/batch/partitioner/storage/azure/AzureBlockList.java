package br.com.spring.batch.partitioner.storage.azure;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.IntStream;

import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.storage.BlockUpload;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.specialized.BlockBlobClient;

public class AzureBlockList implements BlockUpload {

    private static final String BLOCK_ID_FORMAT = "%010d";

    private final BlockBlobClient blob;

    public AzureBlockList(BlockBlobClient blob) {
        this.blob = blob;
    }

    @Override
    public void stage(int blockIndex, byte[] data, int length) {
        blob.stageBlock(blockId(blockIndex), new ByteArrayInputStream(data, 0, length), length);
    }

    @Override
    public void stageFromUrl(int blockIndex, String sourceUrl, ByteRange sourceRange) {
        blob.stageBlockFromUrl(blockId(blockIndex), sourceUrl,
                new BlobRange(sourceRange.start(), sourceRange.length()));
    }

    @Override
    public void commit(int blockCount) {
        blob.commitBlockList(orderedIds(blockCount), true);
    }

    @Override
    public void close() {
    }

    private static List<String> orderedIds(int blockCount) {
        return IntStream.range(0, blockCount).mapToObj(AzureBlockList::blockId).toList();
    }

    private static String blockId(int blockIndex) {
        return Base64.getEncoder().encodeToString(
                BLOCK_ID_FORMAT.formatted(blockIndex).getBytes(StandardCharsets.US_ASCII));
    }
}
