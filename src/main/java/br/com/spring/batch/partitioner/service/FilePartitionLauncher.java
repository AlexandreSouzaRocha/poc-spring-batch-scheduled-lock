package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.config.properties.AppProperties.PartitionSettings;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;

import org.springframework.stereotype.Service;

@Service
public class FilePartitionLauncher {

    private final FilePartitionJobRunner jobRunner;
    private final FileRejectionService rejectionService;
    private final PartitionSettings settings;

    public FilePartitionLauncher(FilePartitionJobRunner jobRunner, FileRejectionService rejectionService,
                                 PartitionSettings settings) {
        this.jobRunner = jobRunner;
        this.rejectionService = rejectionService;
        this.settings = settings;
    }

    public void launch(ReceivedFileDocument file) {
        if (file.attemptsExhausted(settings.maxAttempts())) {
            rejectionService.reject(file.id(), "tentativas esgotadas (" + file.attempts() + "/"
                    + settings.maxAttempts() + ")");
            return;
        }
        JobOutcome outcome = jobRunner.run(file);
        if (outcome == JobOutcome.INVALID_FILE) {
            rejectionService.reject(file.id(), "arquivo inválido");
        }
    }
}
