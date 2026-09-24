package br.com.spring.batch.partitioner.service.dispatch;

public enum ConcurrencyControl {
    CLAIM,
    TYPE_LOCK,
    GLOBAL_LOCK
}
