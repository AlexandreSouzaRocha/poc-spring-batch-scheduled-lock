package br.com.spring.batch.partitioner.batch.step;

public class FileOwnershipLostException extends IllegalStateException {

    public FileOwnershipLostException(String fileName, long jobExecutionId) {
        super("execução " + jobExecutionId + " não é mais a execução corrente do arquivo " + fileName
                + " no JobRepository; job interrompido");
    }
}
