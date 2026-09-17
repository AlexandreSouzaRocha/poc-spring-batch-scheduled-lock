package br.com.spring.batch.partitioner.service.generation;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.IntStream;

import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.layout.FileHeader;
import br.com.spring.batch.partitioner.storage.BlobPaths;

public record GenerationRequest(long lines, MovementType movementType, LocalDate movementDate, int files,
                                boolean invalidHeader) {

    private static final DateTimeFormatter NAME_DATE = DateTimeFormatter.BASIC_ISO_DATE;
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
        return IntStream.rangeClosed(1, files)
                .mapToObj(sequence -> "MOV_" + movementType.name() + "_" + movementDate.format(NAME_DATE) + "_"
                        + timestamp + "_" + String.format("%02d", sequence) + BlobPaths.FILE_EXTENSION)
                .toList();
    }
}
