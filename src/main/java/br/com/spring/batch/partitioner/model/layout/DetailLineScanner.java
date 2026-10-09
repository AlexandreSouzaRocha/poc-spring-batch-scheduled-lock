package br.com.spring.batch.partitioner.model.layout;

public final class DetailLineScanner {

    private static final int HEADER_LINE_NUMBER = 1;

    private final long firstDetailOffset;
    private final int lineBytes;
    private final int detailLength;
    private final byte[] separator;

    public DetailLineScanner(FileLayout layout) {
        this.firstDetailOffset = layout.headerLineBytes();
        this.lineBytes = layout.detailLineBytes();
        this.detailLength = layout.detailLength();
        this.separator = layout.separator().bytes();
    }

    public long scan(byte[] block, int length, long blockOffset) {
        long lines = 0;
        for (int position = firstPositionAt(0, blockOffset); position < length; position += lineBytes) {
            requireDetailIndicator(block[position], blockOffset + position);
            lines++;
        }
        for (int separatorIndex = 0; separatorIndex < separator.length; separatorIndex++) {
            requireSeparatorByte(block, length, blockOffset, separatorIndex);
        }
        return lines;
    }

    private void requireSeparatorByte(byte[] block, int length, long blockOffset, int separatorIndex) {
        int column = detailLength + separatorIndex;
        for (int position = firstPositionAt(column, blockOffset); position < length; position += lineBytes) {
            if (block[position] != separator[separatorIndex]) {
                throw new InvalidFileException("linha " + lineNumberOf(blockOffset + position)
                        + ": quebra de linha fora da posição esperada (byte " + column + " é 0x"
                        + Integer.toHexString(block[position] & 0xFF) + ")");
            }
        }
    }

    private void requireDetailIndicator(byte indicator, long offset) {
        if (!LineType.DETAIL.matches(indicator)) {
            throw new InvalidFileException("linha " + lineNumberOf(offset) + ": indicador "
                    + LineType.describe(indicator) + " onde era esperado 'D' (detalhe)");
        }
    }

    private int firstPositionAt(int column, long blockOffset) {
        long columnOfBlockStart = Math.floorMod(blockOffset - firstDetailOffset, lineBytes);
        return (int) Math.floorMod(column - columnOfBlockStart, (long) lineBytes);
    }

    private long lineNumberOf(long offset) {
        return HEADER_LINE_NUMBER + 1 + (offset - firstDetailOffset) / lineBytes;
    }
}
