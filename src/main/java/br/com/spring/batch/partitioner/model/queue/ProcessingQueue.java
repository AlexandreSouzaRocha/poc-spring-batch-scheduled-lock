package br.com.spring.batch.partitioner.model.queue;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;

public record ProcessingQueue(List<ReceivedFileDocument> files, Optional<ReceivedFileDocument> blockedBy) {

    private static final Comparator<ReceivedFileDocument> QUEUE_ORDER =
            Comparator.comparing(QueuePosition::of);

    public static ProcessingQueue of(List<ReceivedFileDocument> processable, List<ReceivedFileDocument> rejected,
            int limit) {
        Optional<ReceivedFileDocument> blocker = firstInQueue(rejected);
        return new ProcessingQueue(ordered(processable).stream()
                .filter(file -> isReleased(file, blocker))
                .limit(limit)
                .toList(), blocker);
    }

    public static final String UNKNOWN_GROUP = "desconhecido";

    public Map<String, List<ReceivedFileDocument>> byMovementGroup() {
        return files.stream().collect(Collectors.groupingBy(ProcessingQueue::groupOf, LinkedHashMap::new,
                Collectors.toList()));
    }

    private static String groupOf(ReceivedFileDocument file) {
        MovementInfo movement = file.movement();
        if (movement == null) {
            return UNKNOWN_GROUP;
        }
        return movement.type().name().toLowerCase(Locale.ROOT);
    }

    public boolean isEmpty() {
        return files.isEmpty();
    }

    public int size() {
        return files.size();
    }

    private static List<ReceivedFileDocument> ordered(List<ReceivedFileDocument> files) {
        return files.stream().sorted(QUEUE_ORDER).toList();
    }

    private static Optional<ReceivedFileDocument> firstInQueue(List<ReceivedFileDocument> files) {
        return files.stream().min(QUEUE_ORDER);
    }

    private static boolean isReleased(ReceivedFileDocument file, Optional<ReceivedFileDocument> blocker) {
        return blocker.map(blocked -> QueuePosition.of(file).isBefore(QueuePosition.of(blocked)))
                .orElse(true);
    }
}
