package br.com.spring.batch.partitioner.repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.ReceivedFileFields;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Component
public class ReceivedFileCollection {

    private static final String NAME = ReceivedFileDocument.COLLECTION;

    private final MongoTemplate mongoTemplate;

    public ReceivedFileCollection(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    public void createIndex(Index index) {
        mongoTemplate.indexOps(NAME).createIndex(index);
    }

    public boolean insertIfAbsent(ReceivedFileDocument document) {
        try {
            mongoTemplate.insert(document, NAME);
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    public void save(ReceivedFileDocument document) {
        mongoTemplate.save(document, NAME);
    }

    public Optional<ReceivedFileDocument> findById(String id) {
        return Optional.ofNullable(mongoTemplate.findById(id, ReceivedFileDocument.class, NAME));
    }

    public List<ReceivedFileDocument> find(Query query) {
        return mongoTemplate.find(query, ReceivedFileDocument.class, NAME);
    }

    public long count(Criteria criteria) {
        return mongoTemplate.count(query(criteria), NAME);
    }

    public boolean exists(Criteria criteria) {
        return mongoTemplate.exists(query(criteria), NAME);
    }

    public ReceivedFileDocument updateAndGet(String id, Update update) {
        return mongoTemplate.findAndModify(byId(id), touched(update), FindAndModifyOptions.options().returnNew(true),
                ReceivedFileDocument.class, NAME);
    }

    public void update(String id, Update update) {
        mongoTemplate.updateFirst(byId(id), touched(update), NAME);
    }

    public void updateAll(Criteria criteria, Update update) {
        mongoTemplate.updateMulti(query(criteria), touched(update), NAME);
    }

    public long remove(Criteria criteria) {
        return mongoTemplate.remove(query(criteria), NAME).getDeletedCount();
    }

    private static Query byId(String id) {
        return query(where(ReceivedFileFields.ID).is(id));
    }

    private static Update touched(Update update) {
        return update.set(ReceivedFileFields.audit(ReceivedFileFields.UPDATED_AT), Instant.now());
    }
}
