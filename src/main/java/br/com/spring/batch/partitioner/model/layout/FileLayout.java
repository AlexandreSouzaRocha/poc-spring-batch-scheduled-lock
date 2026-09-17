package br.com.spring.batch.partitioner.model.layout;

public final class FileLayout {

    public static final byte HEADER_INDICATOR = 'H';
    public static final byte DETAIL_INDICATOR = 'D';
    public static final byte LINE_SEPARATOR = '\n';

    public static final int DATE_OFFSET = 1;
    public static final int DATE_LENGTH = 10;
    public static final int TYPE_OFFSET = DATE_OFFSET + DATE_LENGTH;
    public static final int TYPE_LENGTH = 7;
    public static final int HEADER_LENGTH = TYPE_OFFSET + TYPE_LENGTH;
    public static final int HEADER_LINE_BYTES = HEADER_LENGTH + 1;

    public static final int RECORD_LENGTH = 150;
    public static final int RECORD_LINE_BYTES = RECORD_LENGTH + 1;

    private FileLayout() {
    }

    public static long detailLineCount(long fileSizeBytes) {
        long detailBytes = fileSizeBytes - HEADER_LINE_BYTES;
        requireAtLeastOneLine(fileSizeBytes, detailBytes);
        requireWholeLines(fileSizeBytes, detailBytes);
        return detailBytes / RECORD_LINE_BYTES;
    }

    public static long byteOffsetOfLine(long lineIndex) {
        return HEADER_LINE_BYTES + lineIndex * RECORD_LINE_BYTES;
    }

    private static void requireAtLeastOneLine(long fileSizeBytes, long detailBytes) {
        if (detailBytes < RECORD_LINE_BYTES) {
            throw new InvalidFileException("arquivo sem linhas de detalhe (" + fileSizeBytes + " bytes)");
        }
    }

    private static void requireWholeLines(long fileSizeBytes, long detailBytes) {
        if (detailBytes % RECORD_LINE_BYTES != 0) {
            throw new InvalidFileException("tamanho " + fileSizeBytes + " incompatível com o layout: header de "
                    + HEADER_LINE_BYTES + " bytes + linhas de " + RECORD_LINE_BYTES + " bytes");
        }
    }
}
