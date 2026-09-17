package br.com.spring.batch.partitioner.lock;

import java.time.Instant;
import java.util.Date;
import java.util.List;

import br.com.spring.batch.partitioner.config.properties.ShedLockProperties;
import br.com.spring.batch.partitioner.config.properties.ShedLockProperties.FieldNames;
import org.bson.Document;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class MongoLockStore {

    private final MongoTemplate mongoTemplate;
    private final ShedLockProperties properties;

    public MongoLockStore(MongoTemplate mongoTemplate, ShedLockProperties properties) {
        this.mongoTemplate = mongoTemplate;
        this.properties = properties;
        ensureUniqueName();
    }

    public boolean acquire(String name, Instant lockUntil, Instant now, String owner) {
        FieldNames fields = properties.fields();
        Query expiredLock = query(where(fields.name()).is(name).and(fields.lockUntil()).lte(Date.from(now)));
        Update ownership = new Update()
                .set(fields.lockUntil(), Date.from(lockUntil))
                .set(fields.lockedAt(), Date.from(now))
                .set(fields.lockedBy(), owner);
        try {
            mongoTemplate.findAndModify(expiredLock, ownership, FindAndModifyOptions.options().upsert(true),
                    Document.class, properties.collection());
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    public boolean extend(String name, Instant lockUntil, Instant now, String owner) {
        FieldNames fields = properties.fields();
        Query ownedActiveLock = query(where(fields.name()).is(name)
                .and(fields.lockUntil()).gt(Date.from(now))
                .and(fields.lockedBy()).is(owner));
        Update extension = new Update().set(fields.lockUntil(), Date.from(lockUntil));
        return mongoTemplate.findAndModify(ownedActiveLock, extension, Document.class, properties.collection()) != null;
    }

    public void release(String name, Instant unlockTime) {
        FieldNames fields = properties.fields();
        mongoTemplate.findAndModify(query(where(fields.name()).is(name)),
                new Update().set(fields.lockUntil(), Date.from(unlockTime)), Document.class, properties.collection());
    }

    public List<Document> findAll() {
        return mongoTemplate.findAll(Document.class, properties.collection());
    }

    public String collection() {
        return properties.collection();
    }

    private void ensureUniqueName() {
        FieldNames fields = properties.fields();
        if (fields.nameIsMongoId()) {
            return;
        }
        mongoTemplate.indexOps(properties.collection()).createIndex(new Index()
                .on(fields.name(), Sort.Direction.ASC)
                .unique()
                .named(fields.name() + "_unique"));
    }
}
