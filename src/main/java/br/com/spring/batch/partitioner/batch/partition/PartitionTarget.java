package br.com.spring.batch.partitioner.batch.partition;

public record PartitionTarget(String path, byte[] header, byte[] trailer) {

    private static final byte[] NO_TRAILER = new byte[0];

    public static PartitionTarget withoutTrailer(String path, byte[] header) {
        return new PartitionTarget(path, header, NO_TRAILER);
    }

    public boolean hasTrailer() {
        return trailer.length > 0;
    }
}
