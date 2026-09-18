package br.com.spring.batch.partitioner.batch.progress;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProgressSnapshotTest {

    private static final long MEGABYTE = 1024 * 1024;

    @Test
    @DisplayName("calcula percentual, throughput e tempo restante")
    void calculatesProgress() {
        ProgressSnapshot snapshot = new ProgressSnapshot(256 * MEGABYTE, 1024 * MEGABYTE, 2000);

        assertThat(snapshot.percent()).isEqualTo(25.0);
        assertThat(snapshot.megabytesPerSecond()).isEqualTo(128.0);
        assertThat(snapshot.remainingSeconds()).isEqualTo(6);
    }

    @Test
    @DisplayName("nao divide por zero quando nada foi copiado")
    void handlesEmptyProgress() {
        ProgressSnapshot snapshot = new ProgressSnapshot(0, 0, 0);

        assertThat(snapshot.percent()).isZero();
        assertThat(snapshot.megabytesPerSecond()).isZero();
        assertThat(snapshot.remainingSeconds()).isZero();
    }
}
