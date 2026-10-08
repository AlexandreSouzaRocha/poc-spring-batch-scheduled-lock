package br.com.spring.batch.partitioner.batch;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import br.com.spring.batch.partitioner.batch.partition.StreamingPartitionCopy;
import br.com.spring.batch.partitioner.batch.progress.PartitionProgressReporter;
import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.config.properties.AppProperties;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.service.dispatch.ConcurrencyControl;
import br.com.spring.batch.partitioner.storage.BlobStorage;
import br.com.spring.batch.partitioner.storage.BlockList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamingPartitionCopyTest {

    private static final int BLOCK_SIZE = 64;
    private static final byte[] HEADER = "H2026-09-18ABERTO \n".getBytes(StandardCharsets.UTF_8);

    private final byte[] source = sourceBytes(5000);
    private final Map<Integer, byte[]> stagedBlocks = new ConcurrentHashMap<>();
    private final BlobStorage storage = mock(BlobStorage.class);
    private final BlockList blocks = mock(BlockList.class);

    @ParameterizedTest(name = "{0} thread(s) por particao")
    @ValueSource(ints = {1, 2, 4, 8})
    @DisplayName("o arquivo final e o header seguido da faixa de bytes, qualquer que seja o paralelismo")
    void writesSameContentRegardlessOfThreads(int threads) {
        ByteRange range = new ByteRange(1000, 4200);
        givenSourceAndRecordedBlocks();
        StreamingPartitionCopy transfer = new StreamingPartitionCopy(storage, BLOCK_SIZE, threads);

        long copied = transfer.copy("origem.txt", range, "destino.txt", HEADER, counter(range.length()));

        assertThat(copied).isEqualTo(range.length());
        assertThat(committed()).isEqualTo(expected(range));
    }

    private void givenSourceAndRecordedBlocks() {
        when(storage.openStream(eq("origem.txt"), any(ByteRange.class))).thenAnswer(invocation -> {
            ByteRange range = invocation.getArgument(1);
            return new ByteArrayInputStream(source, (int) range.start(), (int) range.length());
        });
        when(storage.openBlockList("destino.txt")).thenReturn(blocks);
        doAnswer(invocation -> {
            byte[] data = invocation.getArgument(1);
            int length = invocation.getArgument(2);
            stagedBlocks.put(invocation.getArgument(0), Arrays.copyOf(data, length));
            return null;
        }).when(blocks).stage(anyInt(), any(byte[].class), anyInt());
    }

    private byte[] committed() {
        ArgumentCaptor<Integer> blockCount = ArgumentCaptor.forClass(Integer.class);
        verify(blocks).commit(blockCount.capture());
        return IntStream.range(0, blockCount.getValue())
                .mapToObj(stagedBlocks::get)
                .reduce(new byte[0], StreamingPartitionCopyTest::concat);
    }

    private byte[] expected(ByteRange range) {
        byte[] expected = new byte[HEADER.length + (int) range.length()];
        System.arraycopy(HEADER, 0, expected, 0, HEADER.length);
        System.arraycopy(source, (int) range.start(), expected, HEADER.length, (int) range.length());
        return expected;
    }

    private static ProgressCounter counter(long totalBytes) {
        PartitionSettings settings = new PartitionSettings(10, 3, 1, ConcurrencyControl.TYPE_LOCK, 4, 0, false, 64);
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

    private static byte[] concat(byte[] left, byte[] right) {
        byte[] result = new byte[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }
}
