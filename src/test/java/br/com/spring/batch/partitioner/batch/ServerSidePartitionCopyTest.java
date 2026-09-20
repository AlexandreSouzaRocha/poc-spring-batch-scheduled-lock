package br.com.spring.batch.partitioner.batch;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import br.com.spring.batch.partitioner.batch.partition.ServerSidePartitionCopy;
import br.com.spring.batch.partitioner.batch.progress.PartitionProgressReporter;
import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.config.properties.AppProperties;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.storage.BlobUpload;
import br.com.spring.batch.partitioner.storage.BlobUrls;
import br.com.spring.batch.partitioner.storage.BlobWriter;
import br.com.spring.batch.partitioner.storage.BlockUpload;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class ServerSidePartitionCopyTest {

    private static final int BLOCK_SIZE = 1000;
    private static final byte[] HEADER = "H2026-09-18ABERTO \n".getBytes(StandardCharsets.UTF_8);
    private static final String SOURCE_URL = "http://azurite:10000/conta/movimentos/origem.txt?sig=abc";

    private final RecordingBlockUpload upload = new RecordingBlockUpload();

    @ParameterizedTest(name = "{0} thread(s) por particao")
    @ValueSource(ints = {1, 2, 4})
    @DisplayName("cobre a faixa inteira com blocos contiguos e em ordem, sem ler bytes")
    void stagesContiguousRangesInOrder(int threads) {
        ByteRange range = new ByteRange(1000, 8500);
        ServerSidePartitionCopy copy = new ServerSidePartitionCopy(new FixedBlobUrls(),
                new RecordingBlobWriter(upload), BLOCK_SIZE, threads, Duration.ofHours(1));

        long copied = copy.copy("origem.txt", range, "destino.txt", HEADER, counter(range.length()));

        assertThat(copied).isEqualTo(range.length());
        assertThat(upload.headerBlocks()).containsExactly(0);
        assertThat(upload.orderedRanges()).allSatisfy(staged -> assertThat(staged.url()).isEqualTo(SOURCE_URL));
        assertThat(rangeStarts()).isSorted();
        assertThat(upload.orderedRanges().getFirst().range().start()).isEqualTo(range.start());
        assertThat(upload.orderedRanges().getLast().range().end()).isEqualTo(range.end());
        assertThat(isContiguous()).isTrue();
    }

    private List<Long> rangeStarts() {
        return upload.orderedRanges().stream().map(staged -> staged.range().start()).toList();
    }

    private boolean isContiguous() {
        List<StagedRange> ranges = upload.orderedRanges();
        return java.util.stream.IntStream.range(1, ranges.size())
                .allMatch(index -> ranges.get(index).range().start() == ranges.get(index - 1).range().end());
    }

    private static ProgressCounter counter(long totalBytes) {
        PartitionSettings settings = new PartitionSettings(10, 3, 1, 20, 4, 0, true, 64);
        return new PartitionProgressReporter(new AppProperties(null, null, settings, null, null))
                .track("file", 1, 1, totalBytes);
    }

    private record StagedRange(int blockIndex, String url, ByteRange range) {
    }

    private record FixedBlobUrls() implements BlobUrls {

        @Override
        public String readableUrl(String path, Duration validity) {
            return SOURCE_URL;
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

        private final List<StagedRange> staged = Collections.synchronizedList(new ArrayList<>());
        private final List<Integer> headers = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void stage(int blockIndex, byte[] data, int length) {
            headers.add(blockIndex);
        }

        @Override
        public void stageFromUrl(int blockIndex, String sourceUrl, ByteRange sourceRange) {
            staged.add(new StagedRange(blockIndex, sourceUrl, sourceRange));
        }

        @Override
        public void commit(int blockCount) {
        }

        @Override
        public void close() {
        }

        List<Integer> headerBlocks() {
            return List.copyOf(headers);
        }

        List<StagedRange> orderedRanges() {
            return staged.stream().sorted(Comparator.comparingInt(StagedRange::blockIndex)).toList();
        }
    }
}
