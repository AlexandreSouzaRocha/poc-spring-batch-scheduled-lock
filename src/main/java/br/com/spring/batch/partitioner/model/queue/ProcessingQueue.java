package br.com.spring.batch.partitioner.model.queue;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.enums.MovementType;

public record ProcessingQueue(List<ReceivedFileDocument> files, Optional<ReceivedFileDocument> blockedBy,
        Optional<String> waveDate) {

    public static final String UNKNOWN_GROUP = "desconhecido";

    private static final Comparator<ReceivedFileDocument> QUEUE_ORDER = Comparator.comparing(QueuePosition::of);

    public static ProcessingQueue of(List<ReceivedFileDocument> processable, List<ReceivedFileDocument> rejected,
            MovementDependencies dependencies, int limit) {
        Optional<String> wave = waveOf(processable, rejected);
        Optional<ReceivedFileDocument> blocker = firstInQueue(rejected);
        Set<MovementType> pendingInWave = typesOf(inWave(Stream.concat(processable.stream(), rejected.stream()), wave));
        return new ProcessingQueue(inWave(processable.stream(), wave)
                .filter(file -> isReleased(file, blocker))
                .filter(file -> satisfiesDependencies(file, dependencies, pendingInWave))
                .sorted(QUEUE_ORDER)
                .limit(limit)
                .toList(), blocker, wave);
    }

    public Map<String, List<ReceivedFileDocument>> byMovementGroup() {
        return files.stream().collect(Collectors.groupingBy(ProcessingQueue::groupOf, LinkedHashMap::new,
                Collectors.toList()));
    }

    public boolean isEmpty() {
        return files.isEmpty();
    }

    public int size() {
        return files.size();
    }

    private static Optional<String> waveOf(List<ReceivedFileDocument> processable,
            List<ReceivedFileDocument> rejected) {
        return Stream.concat(processable.stream(), rejected.stream())
                .map(QueuePosition::of)
                .min(Comparator.naturalOrder())
                .map(QueuePosition::movementDate);
    }

    private static Stream<ReceivedFileDocument> inWave(Stream<ReceivedFileDocument> files, Optional<String> wave) {
        return files.filter(file -> wave.map(date -> date.equals(QueuePosition.of(file).movementDate()))
                .orElse(false));
    }

    private static Set<MovementType> typesOf(Stream<ReceivedFileDocument> files) {
        return files.map(ReceivedFileDocument::movement)
                .filter(movement -> movement != null)
                .map(MovementInfo::type)
                .collect(Collectors.toSet());
    }

    private static boolean satisfiesDependencies(ReceivedFileDocument file, MovementDependencies dependencies,
            Set<MovementType> pendingInWave) {
        MovementInfo movement = file.movement();
        if (movement == null) {
            return true;
        }
        return dependencies.isReleased(movement.type(), pendingInWave);
    }

    private static Optional<ReceivedFileDocument> firstInQueue(List<ReceivedFileDocument> files) {
        return files.stream().min(QUEUE_ORDER);
    }

    private static boolean isReleased(ReceivedFileDocument file, Optional<ReceivedFileDocument> blocker) {
        return blocker.map(blocked -> QueuePosition.of(file).isBefore(QueuePosition.of(blocked))).orElse(true);
    }

    private static String groupOf(ReceivedFileDocument file) {
        MovementInfo movement = file.movement();
        if (movement == null) {
            return UNKNOWN_GROUP;
        }
        return movement.type().name().toLowerCase(Locale.ROOT);
    }
}
