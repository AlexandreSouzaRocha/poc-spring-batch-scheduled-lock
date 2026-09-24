package br.com.spring.batch.partitioner.service.dispatch;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import br.com.spring.batch.partitioner.service.FilePartitionLauncher;
import br.com.spring.batch.partitioner.service.InboxFiles;
import br.com.spring.batch.partitioner.storage.BlobFile;
import br.com.spring.batch.partitioner.support.log.RequestContext;

public class ConcurrentDispatch implements InboxDispatch {

    private final FilePartitionLauncher launcher;
    private final GroupGuard guard;
    private final int maxConcurrentTypes;

    public ConcurrentDispatch(FilePartitionLauncher launcher, GroupGuard guard, int maxConcurrentTypes) {
        this.launcher = launcher;
        this.guard = guard;
        this.maxConcurrentTypes = maxConcurrentTypes;
    }

    @Override
    public void dispatch(InboxFiles inbox) {
        Semaphore permits = new Semaphore(maxConcurrentTypes);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            inbox.byMovementGroup().forEach((group, files) ->
                    executor.submit(RequestContext.propagate(() -> launchGroup(group, files, permits))));
        }
    }

    private void launchGroup(String group, List<BlobFile> files, Semaphore permits) {
        permits.acquireUninterruptibly();
        try {
            guard.run(group, () -> files.forEach(launcher::launch));
        } finally {
            permits.release();
        }
    }
}
