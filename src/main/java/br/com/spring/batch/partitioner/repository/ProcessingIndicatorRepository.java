package br.com.spring.batch.partitioner.repository;

import br.com.spring.batch.partitioner.model.document.ProcessingIndicatorDocument;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface ProcessingIndicatorRepository extends MongoRepository<ProcessingIndicatorDocument, String>,
        ProcessingIndicatorRepositoryCustom {
}
