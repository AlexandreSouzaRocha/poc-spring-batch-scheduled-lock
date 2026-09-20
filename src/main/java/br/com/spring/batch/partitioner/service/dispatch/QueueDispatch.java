package br.com.spring.batch.partitioner.service.dispatch;

import br.com.spring.batch.partitioner.model.queue.ProcessingQueue;

public interface QueueDispatch {

    void dispatch(ProcessingQueue queue);
}
