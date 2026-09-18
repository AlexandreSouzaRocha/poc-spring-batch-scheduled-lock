package br.com.spring.batch.partitioner.model;

import java.util.List;

import br.com.spring.batch.partitioner.model.partition.ByteRange;
import br.com.spring.batch.partitioner.model.partition.PartitionChunk;
import br.com.spring.batch.partitioner.model.partition.PartitionChunks;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PartitionChunksTest {

    private static final int BLOCK = 1000;

    @Test
    @DisplayName("divide a faixa em trechos alinhados ao bloco e contiguos")
    void splitsIntoAlignedChunks() {
        PartitionChunks chunks = PartitionChunks.of(new ByteRange(500, 8500), 4, BLOCK, 1);

        assertThat(chunks.size()).isEqualTo(4);
        assertThat(chunks.blockCount()).isEqualTo(8);
        assertThat(chunks.chunks().getFirst().bytes()).isEqualTo(new ByteRange(500, 2500));
        assertThat(chunks.chunks().getLast().bytes()).isEqualTo(new ByteRange(6500, 8500));
        assertThat(totalLength(chunks.chunks())).isEqualTo(8000);
    }

    @Test
    @DisplayName("gera indices de bloco sequenciais a partir do primeiro indice")
    void assignsSequentialBlockIndexes() {
        PartitionChunks chunks = PartitionChunks.of(new ByteRange(0, 10_000), 5, BLOCK, 1);

        assertThat(chunks.chunks().stream().map(PartitionChunk::firstBlockIndex))
                .containsExactly(1, 3, 5, 7, 9);
    }

    @Test
    @DisplayName("mantem um unico trecho quando ha menos blocos que threads")
    void keepsSingleChunkForSmallRange() {
        PartitionChunks chunks = PartitionChunks.of(new ByteRange(0, 700), 8, BLOCK, 1);

        assertThat(chunks.size()).isEqualTo(1);
        assertThat(chunks.blockCount()).isEqualTo(1);
        assertThat(chunks.chunks().getFirst().bytes()).isEqualTo(new ByteRange(0, 700));
    }

    @Test
    @DisplayName("cobre a faixa inteira quando o tamanho nao e multiplo do bloco")
    void coversRangeWithRemainder() {
        PartitionChunks chunks = PartitionChunks.of(new ByteRange(0, 3500), 2, BLOCK, 1);

        assertThat(chunks.blockCount()).isEqualTo(4);
        assertThat(totalLength(chunks.chunks())).isEqualTo(3500);
        assertThat(chunks.chunks().getLast().bytes().end()).isEqualTo(3500);
    }

    private static long totalLength(List<PartitionChunk> chunks) {
        return chunks.stream().mapToLong(chunk -> chunk.bytes().length()).sum();
    }
}
