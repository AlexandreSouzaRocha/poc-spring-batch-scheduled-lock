package br.com.spring.batch.partitioner.repository;

import br.com.spring.batch.partitioner.model.enums.MovementType;

public interface ProcessingIndicatorRepositoryCustom {

    void upsert(String movementDate, MovementType type, String processingDate, long quantity);
}
