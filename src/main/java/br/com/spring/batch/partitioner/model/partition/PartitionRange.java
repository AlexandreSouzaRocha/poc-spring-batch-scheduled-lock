package br.com.spring.batch.partitioner.model.partition;

import br.com.spring.batch.partitioner.model.layout.FileLayout;

import org.springframework.batch.infrastructure.item.ExecutionContext;

public record PartitionRange(int index, long firstLine, long lineCount, ByteRange bytes) {

    private static final String INDEX = "partition.index";
    private static final String FIRST_LINE = "partition.firstLine";
    private static final String LINE_COUNT = "partition.lineCount";
    private static final String BYTE_START = "partition.byteStart";
    private static final String BYTE_END = "partition.byteEnd";

    public static PartitionRange of(int index, long firstLine, long lineCount, FileLayout layout) {
        ByteRange bytes = new ByteRange(layout.byteOffsetOfLine(firstLine),
                layout.byteOffsetOfLine(firstLine + lineCount));
        return new PartitionRange(index, firstLine, lineCount, bytes);
    }

    public static PartitionRange from(ExecutionContext context) {
        return new PartitionRange(context.getInt(INDEX), context.getLong(FIRST_LINE), context.getLong(LINE_COUNT),
                new ByteRange(context.getLong(BYTE_START), context.getLong(BYTE_END)));
    }

    public ExecutionContext toExecutionContext() {
        ExecutionContext context = new ExecutionContext();
        context.putInt(INDEX, index);
        context.putLong(FIRST_LINE, firstLine);
        context.putLong(LINE_COUNT, lineCount);
        context.putLong(BYTE_START, bytes.start());
        context.putLong(BYTE_END, bytes.end());
        return context;
    }

    public long fileSizeBytes(FileLayout layout) {
        return layout.headerLineBytes() + bytes.length();
    }

    public String stepName() {
        return "partition" + String.format("%04d", index);
    }
}
