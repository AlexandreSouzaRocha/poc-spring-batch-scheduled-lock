package br.com.spring.batch.partitioner.service.generation;

public record GeneratedFile(String fileName, String blobPath, String movementType, String movementDate, long lines,
                            long sizeBytes, long elapsedMs, String mbPerSec) {
}
