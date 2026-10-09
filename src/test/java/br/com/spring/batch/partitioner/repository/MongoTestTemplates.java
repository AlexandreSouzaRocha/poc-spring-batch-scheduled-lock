package br.com.spring.batch.partitioner.repository;

import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

final class MongoTestTemplates {

    private MongoTestTemplates() {
    }

    static MongoTemplate withApplicationConversions(MongoDatabaseFactory factory) {
        MongoCustomConversions conversions = MongoEnumConversions.create();
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
        mappingContext.afterPropertiesSet();
        MappingMongoConverter converter = new MappingMongoConverter(new DefaultDbRefResolver(factory), mappingContext);
        converter.setCustomConversions(conversions);
        converter.afterPropertiesSet();
        return new MongoTemplate(factory, converter);
    }
}
