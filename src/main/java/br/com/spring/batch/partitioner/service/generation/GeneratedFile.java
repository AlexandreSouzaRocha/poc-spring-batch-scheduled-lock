package br.com.spring.batch.partitioner.service.generation;

public record GeneratedFile(String fileName, String blobPath, String movementType, String movementDate,
                            String processingDate, long lines, long trailerRecordCount, GenerationDefect defect,
                            long sizeBytes, long elapsedMs, String mbPerSec) {
}
