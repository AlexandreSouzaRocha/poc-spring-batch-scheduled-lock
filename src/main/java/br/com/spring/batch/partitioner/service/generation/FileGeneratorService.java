package br.com.spring.batch.partitioner.service.generation;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import br.com.spring.batch.partitioner.support.log.RequestContext;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("generator")
public class FileGeneratorService {

    private final MovementFileWriter fileWriter;

    public FileGeneratorService(MovementFileWriter fileWriter) {
        this.fileWriter = fileWriter;
    }

    public List<GeneratedFile> generate(GenerationRequest request) {
        String requestId = RequestContext.currentRequestId();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<GeneratedFile>> futures = request.fileNames(System.currentTimeMillis()).stream()
                    .map(fileName -> executor.submit(() -> RequestContext.call(requestId,
                            () -> fileWriter.write(fileName, request))))
                    .toList();
            return futures.stream().map(FileGeneratorService::await).toList();
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
