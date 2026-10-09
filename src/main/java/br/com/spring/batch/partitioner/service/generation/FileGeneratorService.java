package br.com.spring.batch.partitioner.service.generation;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import br.com.spring.batch.partitioner.batch.metrics.StepVolume;
import br.com.spring.batch.partitioner.batch.metrics.Throughput;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.storage.BlobPaths;
import br.com.spring.batch.partitioner.storage.BlobStorage;
import br.com.spring.batch.partitioner.storage.BlockOutputStream;
import br.com.spring.batch.partitioner.support.log.RequestContext;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("generator")
public class FileGeneratorService {

    private static final StructuredLogger log = StructuredLogger.of(FileGeneratorService.class, "generator");

    private final BlobStorage storage;
    private final BlobPaths paths;

    public FileGeneratorService(BlobStorage storage, BlobPaths paths) {
        this.storage = storage;
        this.paths = paths;
    }

    public List<GeneratedFile> generate(GenerationRequest request) {
        String requestId = RequestContext.currentRequestId();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<GeneratedFile>> futures = request.fileNames(System.currentTimeMillis()).stream()
                    .map(fileName -> executor.submit(() -> RequestContext.call(requestId,
                            () -> write(fileName, request))))
                    .toList();
            return futures.stream().map(FileGeneratorService::await).toList();
        }
    }

    private GeneratedFile write(String fileName, GenerationRequest request) {
        String path = paths.inboxPath(fileName);
        long lines = request.lines();
        FileHeader header = request.header();
        long start = System.currentTimeMillis();
        writeContent(path, request);
        long sizeBytes = request.sizeBytes();
        Throughput throughput = new Throughput(new StepVolume(lines, sizeBytes),
                Duration.ofMillis(System.currentTimeMillis() - start));
        log.info("file.generate").field("fileName", fileName).field("lines", lines).field("sizeBytes", sizeBytes)
                .field("elapsedMs", throughput.durationMs()).field("mbPerSec", throughput.megabytesPerSecond())
                .data("blobPath", path).data("header", header.text()).data("invalidHeader", request.invalidHeader())
                .log("arquivo gerado e publicado no blob");
        return new GeneratedFile(fileName, path, header.movementType().name(), header.movementDateText(), lines,
                sizeBytes, throughput.durationMs(), throughput.megabytesPerSecond());
    }

    private void writeContent(String path, GenerationRequest request) {
        DetailRecordBuilder records = new DetailRecordBuilder(request.header().movementDateText(), request.layout());
        try (BlockOutputStream output = storage.openOutput(path)) {
            output.write(request.headerLineBytes());
            writeRecords(output, records, request.lines());
            output.write(request.trailer().lineBytes(request.layout()));
            output.commit();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeRecords(OutputStream output, DetailRecordBuilder records, long lines)
            throws IOException {
        for (long sequence = 1; sequence <= lines; sequence++) {
            output.write(records.next(sequence));
        }
    }

    private static GeneratedFile await(Future<GeneratedFile> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new IllegalStateException("falha ao gerar arquivo", e);
        }
    }
}
