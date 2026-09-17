package br.com.spring.batch.partitioner.support.chaos;

public class SimulatedFailureException extends RuntimeException {

    public SimulatedFailureException(String message) {
        super(message);
    }
}
