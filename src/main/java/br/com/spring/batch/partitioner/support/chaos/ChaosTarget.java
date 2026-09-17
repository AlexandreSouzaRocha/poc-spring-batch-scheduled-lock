package br.com.spring.batch.partitioner.support.chaos;

public record ChaosTarget(ChaosPoint point, String fileId, int attempt, Integer partitionIndex) {

    public static ChaosTarget file(ChaosPoint point, String fileId, int attempt) {
        return new ChaosTarget(point, fileId, attempt, null);
    }

    public static ChaosTarget partition(String fileId, int attempt, int partitionIndex) {
        return new ChaosTarget(ChaosPoint.PARTITION, fileId, attempt, partitionIndex);
    }

    public String describe() {
        return point + " (fileId=" + fileId + ", attempt=" + attempt + ", partition=" + partitionIndex + ")";
    }
}
