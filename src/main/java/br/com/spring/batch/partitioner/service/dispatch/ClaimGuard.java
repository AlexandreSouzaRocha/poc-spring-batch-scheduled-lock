package br.com.spring.batch.partitioner.service.dispatch;

public class ClaimGuard implements GroupGuard {

    @Override
    public void run(String movementGroup, Runnable action) {
        action.run();
    }
}
