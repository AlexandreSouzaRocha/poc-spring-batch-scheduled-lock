package br.com.spring.batch.partitioner.model.queue;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

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
