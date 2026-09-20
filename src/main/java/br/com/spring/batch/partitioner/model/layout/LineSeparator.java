package br.com.spring.batch.partitioner.model.layout;

import java.nio.charset.StandardCharsets;

public enum LineSeparator {
    LF("\n"),
    CRLF("\r\n");

    private final String text;

    LineSeparator(String text) {
        this.text = text;
    }

    public String text() {
        return text;
    }

    public int length() {
        return text.length();
    }

    public byte[] bytes() {
        return text.getBytes(StandardCharsets.US_ASCII);
    }

    public boolean matchesAt(byte[] line, int offset) {
        byte[] expected = bytes();
        if (line.length < offset + expected.length) {
            return false;
        }
        return java.util.Arrays.equals(line, offset, offset + expected.length, expected, 0, expected.length);
    }
}
