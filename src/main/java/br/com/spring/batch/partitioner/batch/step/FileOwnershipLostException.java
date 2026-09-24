package br.com.spring.batch.partitioner.batch.step;

public class FileOwnershipLostException extends IllegalStateException {

    public FileOwnershipLostException(String fileId, long jobExecutionId, Long currentOwner) {
        super("execução " + jobExecutionId + " não é mais dona do arquivo " + fileId
                + " (dono atual: " + currentOwner + "); job interrompido");
    }
}
