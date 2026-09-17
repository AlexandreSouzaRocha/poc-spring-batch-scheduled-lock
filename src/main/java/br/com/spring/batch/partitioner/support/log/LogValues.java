package br.com.spring.batch.partitioner.support.log;

final class LogValues {

    private LogValues() {
    }

    static String format(Object value) {
        String text = String.valueOf(value);
        boolean needsQuotes = text.isEmpty() || text.chars().anyMatch(LogValues::isSpecial);
        return needsQuotes ? quote(text) : text;
    }

    static String quote(String text) {
        return '"' + text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ") + '"';
    }

    private static boolean isSpecial(int character) {
        return Character.isWhitespace(character) || character == '"' || character == '=';
    }
}
