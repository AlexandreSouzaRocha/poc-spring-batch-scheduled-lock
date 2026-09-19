package br.com.spring.batch.partitioner.service.generation;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.IntStream;

import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.storage.BlobPaths;

public record GenerationRequest(long lines, MovementType movementType, LocalDate movementDate, int files,
                                boolean invalidHeader) {

    private static final DateTimeFormatter NAME_DATE = DateTimeFormatter.ofPattern("yyyy.MM.dd");
    private static final DateTimeFormatter NAME_TIME = DateTimeFormatter.ofPattern("HH.mm.ss");
    private static final int MICROS_PER_SECOND = 1_000_000;
    private static final byte INVALID_INDICATOR = 'X';

    public FileHeader header() {
        return new FileHeader(movementDate, movementType);
    }

    public byte[] headerLineBytes() {
        byte[] line = header().lineBytes();
        line[0] = invalidHeader ? INVALID_INDICATOR : line[0];
        return line;
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
