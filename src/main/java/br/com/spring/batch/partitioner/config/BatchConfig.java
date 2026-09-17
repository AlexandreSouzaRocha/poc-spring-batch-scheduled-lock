package br.com.spring.batch.partitioner.config;

import java.util.concurrent.Executors;

import br.com.spring.batch.partitioner.repository.batch.CustomJobRepositoryFactoryBean;
import br.com.spring.batch.partitioner.support.log.RequestContext;
import jakarta.annotation.Nonnull;

import org.springframework.batch.core.configuration.BatchConfigurationException;
import org.springframework.batch.core.configuration.support.MongoDefaultBatchConfiguration;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.task.support.TaskExecutorAdapter;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

@Configuration
public class BatchConfig extends MongoDefaultBatchConfiguration {

    @Bean
    public MongoTransactionManager transactionManager(MongoDatabaseFactory mongoDatabaseFactory) {
        return new MongoTransactionManager(mongoDatabaseFactory);
    }

    @Bean
    @Override
    @Nonnull
    public JobRepository jobRepository() throws BatchConfigurationException {
        var factoryBean = new CustomJobRepositoryFactoryBean();
        try {
            factoryBean.setMongoOperations(getMongoOperations());
            factoryBean.setTransactionManager(getTransactionManager());
            factoryBean.setIsolationLevelForCreateEnum(getIsolationLevelForCreate());
            factoryBean.setValidateTransactionState(getValidateTransactionState());
            factoryBean.setJobKeyGenerator(getJobKeyGenerator());
            factoryBean.afterPropertiesSet();
            return factoryBean.getObject();
        } catch (Exception e) {
            throw new BatchConfigurationException("Unable to configure the custom job repository", e);
        }
    }

    @Bean
    static BeanPostProcessor mongoMapKeyDotReplacement() {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) {
                if (bean instanceof MappingMongoConverter converter) {
                    converter.setMapKeyDotReplacement("#");
                }
                return bean;
            }
        };
    }

    @Bean
    public TaskExecutor partitionTaskExecutor() {
        TaskExecutorAdapter executor = new TaskExecutorAdapter(Executors.newVirtualThreadPerTaskExecutor());
        executor.setTaskDecorator(RequestContext::propagate);
        return executor;
    }
}
