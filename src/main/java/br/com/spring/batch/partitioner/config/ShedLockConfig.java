package br.com.spring.batch.partitioner.config;

import br.com.spring.batch.partitioner.config.properties.ShedLockProperties;
import br.com.spring.batch.partitioner.lock.ConfigurableMongoLockProvider;
import br.com.spring.batch.partitioner.lock.LockKeepAliveExecutor;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import net.javacrumbs.shedlock.core.DefaultLockingTaskExecutor;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
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
    public ConfigurableMongoLockProvider configurableMongoLockProvider(MongoDatabaseFactory mongoDatabaseFactory,
                                                                       ShedLockProperties properties) {
        MongoTemplate lockTemplate = new MongoTemplate(mongoDatabaseFactory);
        lockTemplate.setWriteConcern(WriteConcern.MAJORITY);
        lockTemplate.setReadPreference(ReadPreference.primary());
        return new ConfigurableMongoLockProvider(lockTemplate, properties);
    }

    @Bean
    public LockKeepAliveExecutor lockKeepAliveExecutor() {
        return new LockKeepAliveExecutor();
    }

    @Bean
    public LockingTaskExecutor lockingTaskExecutor(LockProvider lockProvider) {
        return new DefaultLockingTaskExecutor(lockProvider);
    }

    @Bean
    @Primary
    public LockProvider lockProvider(ConfigurableMongoLockProvider configurableMongoLockProvider,
                                     LockKeepAliveExecutor lockKeepAliveExecutor) {
        return new KeepAliveLockProvider(configurableMongoLockProvider, lockKeepAliveExecutor.executor());
    }
}
