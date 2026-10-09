package br.com.spring.batch.partitioner.batch;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import br.com.spring.batch.partitioner.batch.partition.PartitionCopy.CopyResult;
import br.com.spring.batch.partitioner.batch.partition.PartitionTarget;
import br.com.spring.batch.partitioner.batch.partition.StreamingPartitionCopy;
import br.com.spring.batch.partitioner.batch.progress.PartitionProgressReporter;
import br.com.spring.batch.partitioner.batch.progress.ProgressCounter;
import br.com.spring.batch.partitioner.config.properties.AppProperties;
import br.com.spring.batch.partitioner.config.properties.AppProperties.ConcurrencyControl;
import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.layout.DetailLineScanner;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.FileTrailer;
import br.com.spring.batch.partitioner.model.layout.InvalidFileException;
import br.com.spring.batch.partitioner.model.layout.LineSeparator;
import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.model.partition.PartitionPlan;
import br.com.spring.batch.partitioner.model.partition.PartitionRange;
import br.com.spring.batch.partitioner.storage.BlobStorage;
import br.com.spring.batch.partitioner.storage.BlockList;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamingPartitionCopyTest {

    private static final int BLOCK_SIZE = 64;
    private static final int LINES = 200;
    private static final FileLayout LAYOUT = new FileLayout(LineSeparator.LF, 18, 20, 26);
    private static final LocalDate DATE = LocalDate.of(2026, 10, 9);
    private static final byte[] HEADER = FileHeader.of(DATE, MovementType.ABERTO, LAYOUT).lineBytes(LAYOUT);
    private static final byte[] TRAILER = FileTrailer.of(100, DATE, LAYOUT).lineBytes(LAYOUT);

    private final Map<Integer, byte[]> stagedBlocks = new ConcurrentHashMap<>();
    private final BlobStorage storage = mock(BlobStorage.class);
    private final BlockList blocks = mock(BlockList.class);

    @ParameterizedTest(name = "{0} thread(s) por particao")
    @ValueSource(ints = {1, 2, 4, 8})
    @DisplayName("o arquivo final e o header, a faixa de bytes e o trailer, qualquer que seja o paralelismo")
    void writesSameContentRegardlessOfThreads(int threads) {
        byte[] source = file(LINES);
        PartitionRange range = PartitionPlan.split(LINES, 2, LAYOUT).ranges().getLast();
        givenSourceAndRecordedBlocks(source);

        CopyResult copied = copy(threads).copy("origem.txt", range.bytes(),
                new PartitionTarget("destino.txt", HEADER, TRAILER), counter(range.bytes().length()));

        assertThat(copied.copiedBytes()).isEqualTo(range.bytes().length());
        assertThat(copied.verifiedLines()).hasValue(range.lineCount());
        assertThat(committed()).isEqualTo(concat(HEADER, slice(source, range.bytes()), TRAILER));
    }

    @Test
    @DisplayName("sem trailer, a partição termina na última linha de detalhe")
    void writesWithoutTrailerWhenDisabled() {
        byte[] source = file(LINES);
        PartitionRange range = PartitionPlan.split(LINES, 2, LAYOUT).ranges().getFirst();
        givenSourceAndRecordedBlocks(source);

        copy(2).copy("origem.txt", range.bytes(), PartitionTarget.withoutTrailer("destino.txt", HEADER),
                counter(range.bytes().length()));

        assertThat(committed()).isEqualTo(concat(HEADER, slice(source, range.bytes())));
    }

    @Test
    @DisplayName("reprova a partição quando uma linha do meio não começa com D")
    void rejectsLineThatIsNotDetail() {
        byte[] source = file(LINES);
        source[(int) LAYOUT.byteOffsetOfLine(150)] = 'H';
        PartitionRange range = PartitionPlan.split(LINES, 2, LAYOUT).ranges().getLast();
        givenSourceAndRecordedBlocks(source);

        assertThatThrownBy(() -> copy(4).copy("origem.txt", range.bytes(),
                new PartitionTarget("destino.txt", HEADER, TRAILER), counter(range.bytes().length())))
                .rootCause()
                .isInstanceOf(InvalidFileException.class)
                .hasMessageContaining("linha 152")
                .hasMessageContaining("'H' (header)");
    }

    private StreamingPartitionCopy copy(int threads) {
        return new StreamingPartitionCopy(storage, new DetailLineScanner(LAYOUT), BLOCK_SIZE, threads);
    }

    private void givenSourceAndRecordedBlocks(byte[] source) {
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
        return concat(IntStream.range(0, blockCount.getValue()).mapToObj(stagedBlocks::get).toArray(byte[][]::new));
    }

    private static byte[] file(int lines) {
        ByteArrayOutputStream file = new ByteArrayOutputStream();
        file.writeBytes(HEADER);
        IntStream.range(0, lines).forEach(line -> file.writeBytes(
                String.format("D%019d\n", line).getBytes(StandardCharsets.US_ASCII)));
        file.writeBytes(FileTrailer.of(lines, DATE, LAYOUT).lineBytes(LAYOUT));
        return file.toByteArray();
    }

    private static byte[] slice(byte[] source, ByteRange range) {
        return Arrays.copyOfRange(source, (int) range.start(), (int) range.end());
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        Arrays.stream(parts).forEach(result::writeBytes);
        return result.toByteArray();
    }

    private static ProgressCounter counter(long totalBytes) {
        PartitionSettings settings = new PartitionSettings(10, 3, 1, ConcurrencyControl.TYPE_LOCK, 4, 0, false, 64,
                true);
        AppProperties properties = new AppProperties(null, null, settings, null, null);
        return new PartitionProgressReporter(properties).track("file", 1, 1, totalBytes);
    }
}
