package br.com.spring.batch.partitioner.service;

import br.com.spring.batch.partitioner.storage.BlobFile;
import br.com.spring.batch.partitioner.support.log.RequestContext;

import org.springframework.stereotype.Service;

@Service
public class FilePartitionLauncher {

    private final FileClaimService claimService;
    private final FilePartitionJobRunner jobRunner;

    public FilePartitionLauncher(FileClaimService claimService, FilePartitionJobRunner jobRunner) {
        this.claimService = claimService;
        this.jobRunner = jobRunner;
    }

    public void launch(BlobFile file) {
        RequestContext.run(RequestContext.childRequestId(FileClaimService.idOf(file)),
                () -> claimService.claim(file).ifPresent(jobRunner::run));
    }
}
