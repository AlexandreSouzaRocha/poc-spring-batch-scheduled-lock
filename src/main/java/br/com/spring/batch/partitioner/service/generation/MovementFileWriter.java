package br.com.spring.batch.partitioner.service.generation;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.Duration;

import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.batch.metrics.Throughput;
import br.com.spring.batch.partitioner.model.layout.DetailRecordBuilder;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.storage.BlobPaths;
import br.com.spring.batch.partitioner.storage.BlobUpload;
import br.com.spring.batch.partitioner.storage.BlobWriter;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("generator")
public class MovementFileWriter {

    private static final StructuredLogger log = StructuredLogger.of(MovementFileWriter.class, "generator");

    private final BlobWriter writer;
    private final BlobPaths paths;

    public MovementFileWriter(BlobWriter writer, BlobPaths paths) {
        this.writer = writer;
        this.paths = paths;
    }

    public GeneratedFile write(String fileName, GenerationRequest request) {
        String path = paths.inboxPath(fileName);
        long lines = request.lines();
        FileHeader header = request.header();
        long start = System.currentTimeMillis();
        writeContent(path, request);
        long sizeBytes = request.layout().headerLineBytes() + lines * request.layout().recordLineBytes();
        Throughput throughput = new Throughput(new StepVolume(lines, sizeBytes),
                Duration.ofMillis(System.currentTimeMillis() - start));
        GeneratedFile generated = new GeneratedFile(fileName, path, header.movementType().name(),
                header.movementDateText(), lines, sizeBytes, throughput.durationMs(), throughput.megabytesPerSecond());
        log.info("file.generate").field("fileName", fileName).field("lines", lines).field("sizeBytes", sizeBytes)
                .field("elapsedMs", throughput.durationMs()).field("mbPerSec", throughput.megabytesPerSecond())
                .data("blobPath", path).data("header", header.text()).data("invalidHeader", request.invalidHeader())
                .log("arquivo gerado e publicado no blob");
        return generated;
    }

    private void writeContent(String path, GenerationRequest request) {
        DetailRecordBuilder records = new DetailRecordBuilder(request.header().movementDateText(), request.layout());
        try (BlobUpload upload = writer.open(path)) {
            OutputStream output = upload.output();
            output.write(request.headerLineBytes());
            writeRecords(output, records, request.lines());
            upload.commit();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeRecords(OutputStream output, DetailRecordBuilder records, long lines) throws IOException {
        for (long sequence = 1; sequence <= lines; sequence++) {
            output.write(records.next(sequence));
        }
    }
}
