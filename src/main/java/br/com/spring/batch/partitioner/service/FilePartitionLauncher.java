package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.storage.BlobFile;
import br.com.spring.batch.partitioner.support.log.RequestContext;

import org.springframework.stereotype.Service;

@Service
public class FilePartitionLauncher {

    private final FileIntakeService intakeService;
    private final FilePartitionJobRunner jobRunner;

    public FilePartitionLauncher(FileIntakeService intakeService, FilePartitionJobRunner jobRunner) {
        this.intakeService = intakeService;
        this.jobRunner = jobRunner;
    }

    public void launch(BlobFile file) {
        RequestContext.run(RequestContext.childRequestId(FileIntakeService.idOf(file)),
                () -> intakeService.take(file).ifPresent(jobRunner::run));
    }
}
