package br.com.spring.batch.partitioner.model.layout;

import java.time.LocalDate;

public record FileTrailer(long recordCount, LocalDate processingDate, String text) {

    public static final int FIELDS_LENGTH = 26;

    private static final int COUNT_OFFSET = 1;
    private static final int COUNT_LENGTH = 15;
    private static final int DATE_OFFSET = COUNT_OFFSET + COUNT_LENGTH;
    private static final long MAX_RECORD_COUNT = 999_999_999_999_999L;

    public static FileTrailer of(long recordCount, LocalDate processingDate, FileLayout layout) {
        if (recordCount < 0 || recordCount > MAX_RECORD_COUNT) {
            throw new IllegalArgumentException("quantidade de registros fora do campo do trailer: " + recordCount);
        }
        String fields = (char) LineType.TRAILER.indicator() + String.format("%0" + COUNT_LENGTH + "d", recordCount)
                + LineFields.format(processingDate);
        return new FileTrailer(recordCount, processingDate, LineFields.padded(fields, layout.trailerLength()));
    }

    public static FileTrailer parse(byte[] trailerLine, FileLayout layout) {
        LineFields.requireLine(trailerLine, LineType.TRAILER, layout.trailerLength(), layout, "última linha");
        String count = LineFields.ascii(trailerLine, COUNT_OFFSET, COUNT_LENGTH);
        if (!count.chars().allMatch(Character::isDigit)) {
            throw new InvalidFileException("quantidade de registros inválida no trailer: '" + count + "'");
        }
        LocalDate processingDate = LineFields.date(trailerLine, DATE_OFFSET, "data de processamento no trailer");
        return new FileTrailer(Long.parseLong(count), processingDate,
                LineFields.ascii(trailerLine, 0, layout.trailerLength()));
    }

    public String processingDateText() {
        return LineFields.format(processingDate);
    }

    public byte[] lineBytes(FileLayout layout) {
        return LineFields.lineBytes(text, layout);
    }
}
