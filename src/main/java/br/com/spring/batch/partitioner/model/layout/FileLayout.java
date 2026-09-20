package br.com.spring.batch.partitioner.model.layout;

public record FileLayout(LineSeparator separator) {

    public static final byte HEADER_INDICATOR = 'H';
    public static final byte DETAIL_INDICATOR = 'D';

    public static final int DATE_OFFSET = 1;
    public static final int DATE_LENGTH = 10;
    public static final int TYPE_OFFSET = DATE_OFFSET + DATE_LENGTH;
    public static final int TYPE_LENGTH = 7;
    public static final int HEADER_LENGTH = TYPE_OFFSET + TYPE_LENGTH;
    public static final int RECORD_LENGTH = 150;

    public int headerLineBytes() {
        return HEADER_LENGTH + separator.length();
    }

    public int recordLineBytes() {
        return RECORD_LENGTH + separator.length();
    }

    public long detailLineCount(long fileSizeBytes) {
        long detailBytes = fileSizeBytes - headerLineBytes();
        requireAtLeastOneLine(fileSizeBytes, detailBytes);
        requireWholeLines(fileSizeBytes, detailBytes);
        return detailBytes / recordLineBytes();
    }

    public long byteOffsetOfLine(long lineIndex) {
        return headerLineBytes() + lineIndex * recordLineBytes();
    }

    private void requireAtLeastOneLine(long fileSizeBytes, long detailBytes) {
        if (detailBytes >= recordLineBytes()) {
            return;
        }
        throw new InvalidFileException("arquivo sem linhas de detalhe (" + fileSizeBytes + " bytes)");
    }

    private void requireWholeLines(long fileSizeBytes, long detailBytes) {
        if (detailBytes % recordLineBytes() == 0) {
            return;
        }
        throw new InvalidFileException("tamanho " + fileSizeBytes + " incompatível com o layout " + separator
                + ": header de " + headerLineBytes() + " bytes + linhas de " + recordLineBytes() + " bytes");
    }
}
