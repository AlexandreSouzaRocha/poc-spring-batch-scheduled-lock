package br.com.spring.batch.partitioner.lock;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import br.com.spring.batch.partitioner.config.properties.ShedLockProperties;
import br.com.spring.batch.partitioner.config.properties.ShedLockProperties.FieldNames;
import net.javacrumbs.shedlock.core.AbstractSimpleLock;
import net.javacrumbs.shedlock.core.ClockProvider;
import net.javacrumbs.shedlock.core.ExtensibleLockProvider;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.SimpleLock;
import net.javacrumbs.shedlock.support.LockException;
import org.bson.Document;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class ConfigurableMongoLockProvider implements ExtensibleLockProvider {

    private final MongoTemplate mongoTemplate;
    private final ShedLockProperties properties;
    private final FieldNames fields;

    public ConfigurableMongoLockProvider(MongoTemplate mongoTemplate, ShedLockProperties properties) {
        this.mongoTemplate = mongoTemplate;
        this.properties = properties;
        this.fields = properties.fields();
    }

    @Override
    public Optional<SimpleLock> lock(LockConfiguration configuration) {
        try {
            boolean acquired = acquire(configuration.getName(), configuration.getLockAtMostUntil(), ClockProvider.now());
            return acquired ? Optional.of(new MongoLock(configuration, this)) : Optional.empty();
        } catch (DataAccessException e) {
            throw new LockException(e);
        }
    }

    public String owner() {
        return properties.lockedBy();
    }

    public String collection() {
        return properties.collection();
    }

    public List<Document> findAll() {
        return mongoTemplate.findAll(Document.class, collection());
    }

    private boolean acquire(String name, Instant lockUntil, Instant now) {
        Query expiredLock = query(where(fields.name()).is(name).and(fields.lockUntil()).lte(Date.from(now)));
        Update ownership = new Update()
                .set(fields.lockUntil(), Date.from(lockUntil))
                .set(fields.lockedAt(), Date.from(now))
                .set(fields.lockedBy(), owner());
        try {
            mongoTemplate.findAndModify(expiredLock, ownership, FindAndModifyOptions.options().upsert(true),
                    Document.class, collection());
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    private Optional<SimpleLock> extend(LockConfiguration configuration) {
        Instant now = ClockProvider.now();
        Query ownedActiveLock = query(where(fields.name()).is(configuration.getName())
                .and(fields.lockUntil()).gt(Date.from(now))
                .and(fields.lockedBy()).is(owner()));
        Update extension = new Update().set(fields.lockUntil(), Date.from(configuration.getLockAtMostUntil()));
        boolean extended = mongoTemplate.findAndModify(ownedActiveLock, extension, Document.class, collection()) != null;
        return extended ? Optional.of(new MongoLock(configuration, this)) : Optional.empty();
    }

    private void unlock(LockConfiguration configuration) {
        mongoTemplate.findAndModify(query(where(fields.name()).is(configuration.getName())),
                new Update().set(fields.lockUntil(), Date.from(configuration.getUnlockTime())), Document.class,
                collection());
    }

    private static final class MongoLock extends AbstractSimpleLock {

        private final ConfigurableMongoLockProvider provider;

        private MongoLock(LockConfiguration configuration, ConfigurableMongoLockProvider provider) {
            super(configuration);
            this.provider = provider;
        }

        @Override
        protected void doUnlock() {
            provider.unlock(lockConfiguration);
        }

        @Override
        protected Optional<SimpleLock> doExtend(LockConfiguration newConfiguration) {
            return provider.extend(newConfiguration);
        }
    }
}
