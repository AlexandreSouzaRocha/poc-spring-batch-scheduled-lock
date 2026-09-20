package br.com.spring.batch.partitioner.model.queue;

import java.util.Comparator;

import br.com.spring.batch.partitioner.model.document.MovementInfo;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;

public record QueuePosition(String movementDate, int typeOrder, String fileName)
        implements Comparable<QueuePosition> {

    private static final int UNKNOWN_TYPE_ORDER = 0;
    private static final String UNKNOWN_DATE = "";

    private static final Comparator<QueuePosition> ORDER = Comparator.comparing(QueuePosition::movementDate)
            .thenComparingInt(QueuePosition::typeOrder)
            .thenComparing(QueuePosition::fileName);

    public static QueuePosition of(ReceivedFileDocument file) {
        MovementInfo movement = file.movement();
        if (movement == null) {
            return new QueuePosition(UNKNOWN_DATE, UNKNOWN_TYPE_ORDER, file.fileName());
        }
        return new QueuePosition(movement.date(), movement.type().processingOrder(), file.fileName());
    }

    public boolean isBefore(QueuePosition other) {
        return compareTo(other) < 0;
    }

    @Override
    public int compareTo(QueuePosition other) {
        return ORDER.compare(this, other);
    }
}
