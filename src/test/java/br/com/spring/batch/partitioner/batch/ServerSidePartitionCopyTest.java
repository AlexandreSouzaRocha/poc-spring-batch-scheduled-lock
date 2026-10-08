package br.com.spring.batch.partitioner.batch;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

import br.com.spring.batch.partitioner.batch.partition.ServerSidePartitionCopy;
import br.com.spring.batch.partitioner.batch.progress.PartitionProgressReporter;
import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.config.properties.AppProperties;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.config.properties.AppProperties.ConcurrencyControl;
import br.com.spring.batch.partitioner.storage.BlobStorage;
import br.com.spring.batch.partitioner.storage.BlockList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ServerSidePartitionCopyTest {

    private static final int BLOCK_SIZE = 1000;
    private static final byte[] HEADER = "H2026-09-18ABERTO \n".getBytes(StandardCharsets.UTF_8);
    private static final String SOURCE_URL = "http://azurite:10000/conta/movimentos/origem.txt?sig=abc";

    private final List<StagedRange> staged = Collections.synchronizedList(new ArrayList<>());
    private final BlobStorage storage = mock(BlobStorage.class);
    private final BlockList blocks = mock(BlockList.class);

    @ParameterizedTest(name = "{0} thread(s) por particao")
    @ValueSource(ints = {1, 2, 4})
    @DisplayName("cobre a faixa inteira com blocos contiguos e em ordem, sem ler bytes")
    void stagesContiguousRangesInOrder(int threads) {
        ByteRange range = new ByteRange(1000, 8500);
        givenSourceUrlAndRecordedBlocks();
        ServerSidePartitionCopy copy = new ServerSidePartitionCopy(storage, BLOCK_SIZE, threads, Duration.ofHours(1));

        long copied = copy.copy("origem.txt", range, "destino.txt", HEADER, counter(range.length()));

        List<StagedRange> ordered = orderedRanges();
        assertThat(copied).isEqualTo(range.length());
        verify(blocks).stage(eq(0), eq(HEADER), eq(HEADER.length));
        assertThat(ordered).allSatisfy(block -> assertThat(block.url()).isEqualTo(SOURCE_URL));
        assertThat(ordered.stream().map(block -> block.range().start()).toList()).isSorted();
        assertThat(ordered.getFirst().range().start()).isEqualTo(range.start());
        assertThat(ordered.getLast().range().end()).isEqualTo(range.end());
        assertThat(isContiguous(ordered)).isTrue();
    }

    private void givenSourceUrlAndRecordedBlocks() {
        when(storage.readableUrl(eq("origem.txt"), any(Duration.class))).thenReturn(SOURCE_URL);
        when(storage.openBlockList("destino.txt")).thenReturn(blocks);
        doAnswer(invocation -> staged.add(new StagedRange(invocation.getArgument(0), invocation.getArgument(1),
                invocation.getArgument(2))))
                .when(blocks).stageFromUrl(anyInt(), anyString(), any(ByteRange.class));
    }

    private List<StagedRange> orderedRanges() {
        return staged.stream().sorted(Comparator.comparingInt(StagedRange::blockIndex)).toList();
    }

    private static boolean isContiguous(List<StagedRange> ranges) {
        return IntStream.range(1, ranges.size())
                .allMatch(index -> ranges.get(index).range().start() == ranges.get(index - 1).range().end());
    }

    private static ProgressCounter counter(long totalBytes) {
        PartitionSettings settings = new PartitionSettings(10, 3, 1, ConcurrencyControl.TYPE_LOCK, 4, 0, true, 64);
        return new PartitionProgressReporter(new AppProperties(null, null, settings, null, null))
                .track("file", 1, 1, totalBytes);
    }

    private record StagedRange(int blockIndex, String url, ByteRange range) {
    }
}
