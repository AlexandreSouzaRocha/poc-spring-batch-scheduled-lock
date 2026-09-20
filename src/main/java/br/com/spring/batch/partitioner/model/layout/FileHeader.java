package br.com.spring.batch.partitioner.model.layout;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;

import br.com.spring.batch.partitioner.model.enums.MovementType;

public record FileHeader(LocalDate movementDate, MovementType movementType) {

    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd").withResolverStyle(ResolverStyle.STRICT);

    public static FileHeader parse(byte[] headerLine, FileLayout layout) {
        requireLength(headerLine);
        requireIndicator(headerLine);
        requireLineEnd(headerLine, layout);
        String type = ascii(headerLine, FileLayout.TYPE_OFFSET, FileLayout.TYPE_LENGTH);
        return new FileHeader(parseDate(ascii(headerLine, FileLayout.DATE_OFFSET, FileLayout.DATE_LENGTH)),
                MovementType.fromHeaderField(type));
    }

    public String text() {
        return (char) FileLayout.HEADER_INDICATOR + movementDateText()
                + String.format("%-" + FileLayout.TYPE_LENGTH + "s", movementType.name());
    }

    public byte[] lineBytes(FileLayout layout) {
        return (text() + layout.separator().text()).getBytes(StandardCharsets.US_ASCII);
    }

    public String movementDateText() {
        return movementDate.format(DATE_FORMAT);
    }

    private static void requireLength(byte[] headerLine) {
        if (headerLine.length < FileLayout.HEADER_LENGTH) {
            throw new InvalidFileException("header com " + headerLine.length + " bytes; esperado "
                    + FileLayout.HEADER_LENGTH);
        }
    }

    private static void requireIndicator(byte[] headerLine) {
        if (headerLine[0] != FileLayout.HEADER_INDICATOR) {
            throw new InvalidFileException("primeira linha não é header: indicador '" + (char) headerLine[0]
                    + "', esperado 'H'");
        }
    }

    private static void requireLineEnd(byte[] headerLine, FileLayout layout) {
        boolean hasLineEnd = headerLine.length > FileLayout.HEADER_LENGTH;
        if (!hasLineEnd || layout.separator().matchesAt(headerLine, FileLayout.HEADER_LENGTH)) {
            return;
        }
        throw new InvalidFileException("quebra de linha do arquivo não corresponde ao layout configurado ("
                + layout.separator() + "): byte " + FileLayout.HEADER_LENGTH + " é 0x"
                + Integer.toHexString(headerLine[FileLayout.HEADER_LENGTH] & 0xFF));
    }

    private static LocalDate parseDate(String date) {
        try {
            return LocalDate.parse(date, DATE_FORMAT);
        } catch (DateTimeParseException e) {
            throw new InvalidFileException("data do movimento inválida no header: '" + date + "'");
        }
    }

    private static String ascii(byte[] bytes, int offset, int length) {
        return new String(bytes, offset, length, StandardCharsets.US_ASCII);
    }
}
