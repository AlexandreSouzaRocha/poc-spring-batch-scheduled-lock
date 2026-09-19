package br.com.spring.batch.partitioner.model.layout;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import br.com.spring.batch.partitioner.model.enums.MovementType;

public record MovementFileName(MovementType type, String movementDate) {

    private static final Pattern PATTERN =
            Pattern.compile("^MOV_([A-Za-z]+)_(\\d{4})\\.(\\d{2})\\.(\\d{2})\\..+\\.txt$");
    private static final String DATE_SEPARATOR = "-";

    public static Optional<MovementFileName> parse(String fileName) {
        Matcher matcher = PATTERN.matcher(fileName);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        return typeOf(matcher.group(1))
                .map(type -> new MovementFileName(type, dateOf(matcher)));
    }

    private static Optional<MovementType> typeOf(String rawType) {
        String value = rawType.toUpperCase(Locale.ROOT);
        return Arrays.stream(MovementType.values()).filter(type -> type.name().equals(value)).findFirst();
    }

    private static String dateOf(Matcher matcher) {
        return matcher.group(2) + DATE_SEPARATOR + matcher.group(3) + DATE_SEPARATOR + matcher.group(4);
    }
}
