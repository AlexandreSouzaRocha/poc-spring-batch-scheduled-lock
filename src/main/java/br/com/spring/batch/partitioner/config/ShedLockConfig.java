package br.com.spring.batch.partitioner.config;

import br.com.spring.batch.partitioner.config.properties.ShedLockProperties;
import br.com.spring.batch.partitioner.lock.ConfigurableMongoLockProvider;
import br.com.spring.batch.partitioner.lock.LockKeepAliveExecutor;
import br.com.spring.batch.partitioner.lock.MongoLockStore;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import net.javacrumbs.shedlock.support.KeepAliveLockProvider;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;

@Configuration
@EnableSchedulerLock(defaultLockAtMostFor = "${app.shedlock.default-lock-at-most-for}")
public class ShedLockConfig {

    @Bean
    public MongoLockStore mongoLockStore(MongoDatabaseFactory mongoDatabaseFactory, ShedLockProperties properties) {
        MongoTemplate lockTemplate = new MongoTemplate(mongoDatabaseFactory);
        lockTemplate.setWriteConcern(WriteConcern.MAJORITY);
        lockTemplate.setReadPreference(ReadPreference.primary());
        return new MongoLockStore(lockTemplate, properties);
    }

    @Bean
    public ConfigurableMongoLockProvider configurableMongoLockProvider(MongoLockStore mongoLockStore,
                                                                       ShedLockProperties properties) {
        return new ConfigurableMongoLockProvider(mongoLockStore, properties.lockedBy());
    }

    @Bean
    public LockKeepAliveExecutor lockKeepAliveExecutor() {
        return new LockKeepAliveExecutor();
    }

    @Bean
    @Primary
    public LockProvider lockProvider(ConfigurableMongoLockProvider configurableMongoLockProvider,
                                     LockKeepAliveExecutor lockKeepAliveExecutor) {
        return new KeepAliveLockProvider(configurableMongoLockProvider, lockKeepAliveExecutor.executor());
    }
}
