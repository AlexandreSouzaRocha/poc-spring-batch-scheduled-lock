package br.com.spring.batch.partitioner.model.layout;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;

final class LineFields {

    static final int DATE_LENGTH = 10;

    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("uuuu-MM-dd").withResolverStyle(ResolverStyle.STRICT);

    private LineFields() {
    }

    static void requireLine(byte[] line, LineType type, int length, FileLayout layout, String position) {
        if (line.length < length) {
            throw new InvalidFileException(type.description() + " com " + line.length + " bytes; esperado "
                    + length);
        }
        if (!type.matches(line[0])) {
            throw new InvalidFileException(position + " não é " + type.description() + ": indicador "
                    + LineType.describe(line[0]) + ", esperado '" + (char) type.indicator() + "'");
        }
        boolean hasLineEnd = line.length > length;
        if (hasLineEnd && !layout.separator().matchesAt(line, length)) {
            throw new InvalidFileException("quebra de linha do " + type.description()
                    + " não corresponde ao layout configurado (" + layout.separator() + "): byte " + length
                    + " é 0x" + Integer.toHexString(line[length] & 0xFF));
        }
    }

    static String ascii(byte[] line, int offset, int length) {
        return new String(line, offset, length, StandardCharsets.US_ASCII);
    }

    static LocalDate date(byte[] line, int offset, String description) {
        String text = ascii(line, offset, DATE_LENGTH);
        try {
            return LocalDate.parse(text, DATE_FORMAT);
        } catch (DateTimeParseException e) {
            throw new InvalidFileException(description + " inválida: '" + text + "'");
        }
    }

    static String format(LocalDate date) {
        return date.format(DATE_FORMAT);
    }

    static String padded(String fields, int length) {
        return String.format("%-" + length + "s", fields);
    }

    static byte[] lineBytes(String text, FileLayout layout) {
        return (text + layout.separator().text()).getBytes(StandardCharsets.US_ASCII);
    }
}
