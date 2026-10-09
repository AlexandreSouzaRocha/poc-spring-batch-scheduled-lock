package br.com.spring.batch.partitioner.model.layout;

import br.com.spring.batch.partitioner.model.partition.ByteRange;

public record FileLayout(LineSeparator separator, int headerLength, int detailLength, int trailerLength) {

    public static final int DEFAULT_DETAIL_LENGTH = 150;

    public FileLayout {
        requireAtLeast("header", headerLength, FileHeader.FIELDS_LENGTH);
        requireAtLeast("detalhe", detailLength, 1);
        requireAtLeast("trailer", trailerLength, FileTrailer.FIELDS_LENGTH);
    }

    public static FileLayout standard(LineSeparator separator) {
        return new FileLayout(separator, FileHeader.FIELDS_LENGTH, DEFAULT_DETAIL_LENGTH, FileTrailer.FIELDS_LENGTH);
    }

    public int headerLineBytes() {
        return headerLength + separator.length();
    }

    public int detailLineBytes() {
        return detailLength + separator.length();
    }

    public int trailerLineBytes() {
        return trailerLength + separator.length();
    }

    public ByteRange headerRange() {
        return new ByteRange(0, headerLineBytes());
    }

    public ByteRange trailerRange(long detailLineCount) {
        long start = byteOffsetOfLine(detailLineCount);
        return new ByteRange(start, start + trailerLength);
    }

    public long byteOffsetOfLine(long lineIndex) {
        return headerLineBytes() + lineIndex * detailLineBytes();
    }

    public long detailLineCount(long fileSizeBytes) {
        long detailBytes = fileSizeBytes - headerLineBytes() - trailerLength;
        if (detailBytes < detailLineBytes()) {
            throw new InvalidFileException("arquivo sem linhas de detalhe (" + fileSizeBytes + " bytes)");
        }
        if (detailBytes % detailLineBytes() == 0) {
            return detailBytes / detailLineBytes();
        }
        long withoutFinalSeparator = detailBytes - separator.length();
        if (withoutFinalSeparator % detailLineBytes() == 0) {
            return withoutFinalSeparator / detailLineBytes();
        }
        throw new InvalidFileException("tamanho " + fileSizeBytes + " incompatível com o layout " + separator
                + ": header de " + headerLineBytes() + " bytes + linhas de " + detailLineBytes()
                + " bytes + trailer de " + trailerLength + " bytes, com quebra final opcional");
    }

    private static void requireAtLeast(String line, int length, int minimum) {
        if (length < minimum) {
            throw new IllegalArgumentException("tamanho da linha de " + line + " é " + length + "; mínimo "
                    + minimum);
        }
    }
}
