package br.com.spring.batch.partitioner.model.layout;

import java.time.LocalDate;

import br.com.spring.batch.partitioner.model.enums.MovementType;

public record FileHeader(LocalDate movementDate, MovementType movementType, String text) {

    public static final int FIELDS_LENGTH = 18;

    private static final int DATE_OFFSET = 1;
    private static final int TYPE_OFFSET = DATE_OFFSET + LineFields.DATE_LENGTH;
    private static final int TYPE_LENGTH = 7;

    public static FileHeader of(LocalDate movementDate, MovementType movementType, FileLayout layout) {
        String fields = (char) LineType.HEADER.indicator() + LineFields.format(movementDate)
                + LineFields.padded(movementType.name(), TYPE_LENGTH);
        return new FileHeader(movementDate, movementType, LineFields.padded(fields, layout.headerLength()));
    }

    public static FileHeader parse(byte[] headerLine, FileLayout layout) {
        LineFields.requireLine(headerLine, LineType.HEADER, layout.headerLength(), layout, "primeira linha");
        LocalDate movementDate = LineFields.date(headerLine, DATE_OFFSET, "data do movimento no header");
        MovementType movementType = MovementType.fromHeaderField(LineFields.ascii(headerLine, TYPE_OFFSET,
                TYPE_LENGTH));
        return new FileHeader(movementDate, movementType, LineFields.ascii(headerLine, 0, layout.headerLength()));
    }

    public String movementDateText() {
        return LineFields.format(movementDate);
    }

    public byte[] lineBytes(FileLayout layout) {
        return LineFields.lineBytes(text, layout);
    }
}
