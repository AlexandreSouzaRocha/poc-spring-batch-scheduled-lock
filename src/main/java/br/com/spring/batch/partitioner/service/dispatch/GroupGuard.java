package br.com.spring.batch.partitioner.service.dispatch;

public interface GroupGuard {

    void run(String movementGroup, Runnable action);
}
