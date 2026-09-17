package br.com.spring.batch.partitioner.model.partition;

public record ByteRange(long start, long end) {

    public long length() {
        return end - start;
    }

    public boolean isFullyCopied(long copiedBytes) {
        return copiedBytes == length();
    }
}
