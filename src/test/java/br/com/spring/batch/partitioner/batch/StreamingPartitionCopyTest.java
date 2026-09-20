package br.com.spring.batch.partitioner.batch;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import br.com.spring.batch.partitioner.batch.partition.StreamingPartitionCopy;
import br.com.spring.batch.partitioner.batch.progress.PartitionProgressReporter;
import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.config.properties.AppProperties;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.storage.BlobReader;
import br.com.spring.batch.partitioner.storage.BlobUpload;
import br.com.spring.batch.partitioner.storage.BlobWriter;
import br.com.spring.batch.partitioner.storage.BlockUpload;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class StreamingPartitionCopyTest {

    private static final int BLOCK_SIZE = 64;
    private static final byte[] HEADER = "H2026-09-18ABERTO \n".getBytes(StandardCharsets.UTF_8);

    private final byte[] source = sourceBytes(5000);
    private final RecordingBlockUpload upload = new RecordingBlockUpload();

    @ParameterizedTest(name = "{0} thread(s) por particao")
    @ValueSource(ints = {1, 2, 4, 8})
    @DisplayName("o arquivo final e o header seguido da faixa de bytes, qualquer que seja o paralelismo")
    void writesSameContentRegardlessOfThreads(int threads) {
        ByteRange range = new ByteRange(1000, 4200);
        StreamingPartitionCopy transfer = new StreamingPartitionCopy(new ArrayBlobReader(source), new RecordingBlobWriter(upload),
                BLOCK_SIZE, threads);

        long copied = transfer.copy("origem.txt", range, "destino.txt", HEADER, counter(range.length()));

        assertThat(copied).isEqualTo(range.length());
        assertThat(upload.committed()).isEqualTo(expected(range));
    }

    private byte[] expected(ByteRange range) {
        byte[] expected = new byte[HEADER.length + (int) range.length()];
        System.arraycopy(HEADER, 0, expected, 0, HEADER.length);
        System.arraycopy(source, (int) range.start(), expected, HEADER.length, (int) range.length());
        return expected;
    }

    private static ProgressCounter counter(long totalBytes) {
        PartitionSettings settings = new PartitionSettings(10, 3, 1, 20, 4, 0, false, 64);
        AppProperties properties = new AppProperties(null, null, settings, null, null);
        return new PartitionProgressReporter(properties).track("file", 1, 1, totalBytes);
    }

    private static byte[] sourceBytes(int length) {
        byte[] bytes = new byte[length];
        for (int index = 0; index < length; index++) {
            bytes[index] = (byte) (index % 251);
        }
        return bytes;
    }

    private record ArrayBlobReader(byte[] source) implements BlobReader {

        @Override
        public byte[] read(String path, ByteRange range) {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream openStream(String path, ByteRange range) {
            return new ByteArrayInputStream(source, (int) range.start(), (int) range.length());
        }

        @Override
        public long copy(String path, ByteRange range, OutputStream target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void copyAll(String path, OutputStream target) {
            throw new UnsupportedOperationException();
        }
    }

    private record RecordingBlobWriter(BlockUpload upload) implements BlobWriter {

        @Override
        public BlobUpload open(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockUpload openBlocks(String path) {
            return upload;
        }
    }

    private static final class RecordingBlockUpload implements BlockUpload {

        private final Map<Integer, byte[]> blocks = new ConcurrentHashMap<>();
        private int committedBlocks;

        @Override
        public void stage(int blockIndex, byte[] data, int length) {
            byte[] copy = new byte[length];
            System.arraycopy(data, 0, copy, 0, length);
            blocks.put(blockIndex, copy);
        }

        @Override
        public void stageFromUrl(int blockIndex, String sourceUrl, ByteRange sourceRange) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void commit(int blockCount) {
            committedBlocks = blockCount;
        }

        @Override
        public void close() {
        }

        byte[] committed() {
            return java.util.stream.IntStream.range(0, committedBlocks)
                    .mapToObj(blocks::get)
                    .reduce(new byte[0], RecordingBlockUpload::concat);
        }

        private static byte[] concat(byte[] left, byte[] right) {
            byte[] result = new byte[left.length + right.length];
            System.arraycopy(left, 0, result, 0, left.length);
            System.arraycopy(right, 0, result, left.length, right.length);
            return result;
        }
    }
}
