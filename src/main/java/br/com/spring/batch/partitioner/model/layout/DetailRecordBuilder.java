package br.com.spring.batch.partitioner.model.layout;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public final class DetailRecordBuilder {

    private static final int AGENCY_OFFSET = 1;
    private static final int AGENCY_WIDTH = 4;
    private static final int ACCOUNT_OFFSET = 5;
    private static final int ACCOUNT_WIDTH = 10;
    private static final int DATE_OFFSET = 15;
    private static final int SIGN_OFFSET = 25;
    private static final int AMOUNT_OFFSET = 26;
    private static final int AMOUNT_WIDTH = 15;
    private static final int SEQUENCE_OFFSET = 41;
    private static final int SEQUENCE_WIDTH = 12;
    private static final int FILLER_OFFSET = 53;
    private static final int MAX_AGENCY = 10_000;
    private static final long MAX_ACCOUNT = 10_000_000_000L;
    private static final long MAX_AMOUNT = 100_000_000_000L;
    private static final int DECIMAL_BASE = 10;

    private final byte[] line;

    public DetailRecordBuilder(String movementDate, FileLayout layout) {
        this.line = new byte[layout.recordLineBytes()];
        line[0] = FileLayout.DETAIL_INDICATOR;
        byte[] date = movementDate.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(date, 0, line, DATE_OFFSET, date.length);
        byte[] separator = layout.separator().bytes();
        System.arraycopy(separator, 0, line, FileLayout.RECORD_LENGTH, separator.length);
    }

    public byte[] next(long sequence) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        writeDigits(AGENCY_OFFSET, AGENCY_WIDTH, random.nextInt(MAX_AGENCY));
        writeDigits(ACCOUNT_OFFSET, ACCOUNT_WIDTH, random.nextLong(MAX_ACCOUNT));
        line[SIGN_OFFSET] = random.nextBoolean() ? (byte) '+' : (byte) '-';
        writeDigits(AMOUNT_OFFSET, AMOUNT_WIDTH, random.nextLong(MAX_AMOUNT));
        writeDigits(SEQUENCE_OFFSET, SEQUENCE_WIDTH, sequence);
        fillRandomDigits(random);
        return line;
    }

    private void fillRandomDigits(ThreadLocalRandom random) {
        for (int position = FILLER_OFFSET; position < FileLayout.RECORD_LENGTH; position++) {
            line[position] = digit(random.nextInt(DECIMAL_BASE));
        }
    }

    private void writeDigits(int offset, int width, long value) {
        long remaining = value;
        for (int position = offset + width - 1; position >= offset; position--) {
            line[position] = digit(remaining % DECIMAL_BASE);
            remaining /= DECIMAL_BASE;
        }
    }

    private static byte digit(long value) {
        return (byte) ('0' + value);
    }
}
