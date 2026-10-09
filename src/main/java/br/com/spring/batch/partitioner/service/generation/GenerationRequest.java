package br.com.spring.batch.partitioner.service.generation;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.IntStream;

import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.model.layout.FileTrailer;
import br.com.spring.batch.partitioner.storage.BlobPaths;

public record GenerationRequest(long lines, MovementType movementType, LocalDate movementDate,
                                LocalDate processingDate, int files, GenerationDefect defect, FileLayout layout) {

    private static final DateTimeFormatter NAME_DATE = DateTimeFormatter.ofPattern("yyyy.MM.dd");
    private static final DateTimeFormatter NAME_TIME = DateTimeFormatter.ofPattern("HH.mm.ss");
    private static final int MICROS_PER_SECOND = 1_000_000;
    private static final byte INVALID_INDICATOR = 'X';
    private static final long NO_CORRUPTED_LINE = 0;

    public FileHeader header() {
        return FileHeader.of(movementDate, movementType, layout);
    }

    public byte[] headerLineBytes() {
        byte[] line = header().lineBytes(layout);
        if (defect == GenerationDefect.HEADER) {
            line[0] = INVALID_INDICATOR;
        }
        return line;
    }

    public FileTrailer trailer() {
        long recordCount = defect == GenerationDefect.COUNT ? lines + 1 : lines;
        return FileTrailer.of(recordCount, processingDate, layout);
    }

    public byte[] trailerLineBytes() {
        byte[] line = trailer().lineBytes(layout);
        if (defect == GenerationDefect.TRAILER) {
            line[0] = INVALID_INDICATOR;
        }
        return line;
    }

    public long corruptedDetailLine() {
        return defect == GenerationDefect.DETAIL ? lines / 2 + 1 : NO_CORRUPTED_LINE;
    }

    public long sizeBytes() {
        return layout.headerLineBytes() + lines * layout.detailLineBytes() + layout.trailerLineBytes();
    }

    public List<String> fileNames(long timestamp) {
        LocalTime time = LocalTime.now();
        return IntStream.rangeClosed(1, files)
                .mapToObj(sequence -> "MOV_" + movementType.name() + "_" + movementDate.format(NAME_DATE) + "."
                        + time.format(NAME_TIME) + "." + microseconds(timestamp, sequence)
                        + BlobPaths.FILE_EXTENSION)
                .toList();
    }

    private static String microseconds(long timestamp, int sequence) {
        return String.format("%06d", Math.floorMod(timestamp + sequence, MICROS_PER_SECOND));
    }
}
