package br.com.spring.batch.partitioner.controller;

import java.time.Instant;
import java.util.List;

import br.com.spring.batch.partitioner.lock.ConfigurableMongoLockProvider;
import org.bson.Document;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LockController {

    private final ConfigurableMongoLockProvider provider;

    public LockController(ConfigurableMongoLockProvider provider) {
        this.provider = provider;
    }

    @GetMapping("/locks")
    public LockView locks() {
        return new LockView(provider.owner(), provider.collection(), Instant.now(), provider.findAll());
    }

    public record LockView(String instance, String collection, Instant now, List<Document> locks) {
    }
}
