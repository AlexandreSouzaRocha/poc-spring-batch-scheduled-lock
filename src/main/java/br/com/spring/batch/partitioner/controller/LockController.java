package br.com.spring.batch.partitioner.controller;

import java.time.Instant;
import java.util.List;

import br.com.spring.batch.partitioner.lock.ConfigurableMongoLockProvider;
import br.com.spring.batch.partitioner.lock.MongoLockStore;
import org.bson.Document;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LockController {

    private final MongoLockStore store;
    private final ConfigurableMongoLockProvider provider;

    public LockController(MongoLockStore store, ConfigurableMongoLockProvider provider) {
        this.store = store;
        this.provider = provider;
    }

    @GetMapping("/locks")
    public LockView locks() {
        return new LockView(provider.owner(), store.collection(), Instant.now(), store.findAll());
    }

    public record LockView(String instance, String collection, Instant now, List<Document> locks) {
    }
}
