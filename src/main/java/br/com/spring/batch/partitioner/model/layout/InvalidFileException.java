package br.com.spring.batch.partitioner.model.layout;

public class InvalidFileException extends RuntimeException {

    public InvalidFileException(String message) {
        super(message);
    }
}
