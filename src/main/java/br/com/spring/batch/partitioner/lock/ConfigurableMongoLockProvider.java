package br.com.spring.batch.partitioner.lock;

import java.util.Optional;

import net.javacrumbs.shedlock.core.AbstractSimpleLock;
import net.javacrumbs.shedlock.core.ClockProvider;
import net.javacrumbs.shedlock.core.ExtensibleLockProvider;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.SimpleLock;
import net.javacrumbs.shedlock.support.LockException;

import org.springframework.dao.DataAccessException;

public class ConfigurableMongoLockProvider implements ExtensibleLockProvider {

    private final MongoLockStore store;
    private final String owner;

    public ConfigurableMongoLockProvider(MongoLockStore store, String owner) {
        this.store = store;
        this.owner = owner;
    }

    @Override
    public Optional<SimpleLock> lock(LockConfiguration configuration) {
        try {
            boolean acquired = store.acquire(configuration.getName(), configuration.getLockAtMostUntil(),
                    ClockProvider.now(), owner);
            return acquired ? Optional.of(new MongoLock(configuration, this)) : Optional.empty();
        } catch (DataAccessException e) {
            throw new LockException(e);
        }
    }

    public String owner() {
        return owner;
    }

    private Optional<SimpleLock> extend(LockConfiguration configuration) {
        boolean extended = store.extend(configuration.getName(), configuration.getLockAtMostUntil(),
                ClockProvider.now(), owner);
        return extended ? Optional.of(new MongoLock(configuration, this)) : Optional.empty();
    }

    private void unlock(LockConfiguration configuration) {
        store.release(configuration.getName(), configuration.getUnlockTime());
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
