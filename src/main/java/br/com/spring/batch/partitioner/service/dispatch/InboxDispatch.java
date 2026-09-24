package br.com.spring.batch.partitioner.service.dispatch;

import br.com.spring.batch.partitioner.service.InboxFiles;

public interface InboxDispatch {

    void dispatch(InboxFiles inbox);
}
