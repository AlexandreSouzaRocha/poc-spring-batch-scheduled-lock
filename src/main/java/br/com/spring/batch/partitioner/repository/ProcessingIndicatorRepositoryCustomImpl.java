package br.com.spring.batch.partitioner.repository;

import br.com.spring.batch.partitioner.model.document.ProcessingIndicatorDocument;
import br.com.spring.batch.partitioner.model.document.ProcessingIndicatorFields;
import br.com.spring.batch.partitioner.model.enums.MovementType;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Update;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class ProcessingIndicatorRepositoryCustomImpl implements ProcessingIndicatorRepositoryCustom {

    private static final String ID = "_id";

    private final MongoTemplate mongoTemplate;

    public ProcessingIndicatorRepositoryCustomImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void upsert(String movementDate, MovementType type, String processingDate, long quantity) {
        mongoTemplate.upsert(query(where(ID).is(movementDate)), new Update()
                .set(ProcessingIndicatorFields.processingDateOf(type), processingDate)
                .set(ProcessingIndicatorFields.quantityOf(type), quantity), ProcessingIndicatorDocument.class);
    }
}
