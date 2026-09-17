package br.com.spring.batch.partitioner.controller;

import java.util.Map;

import br.com.spring.batch.partitioner.support.chaos.ChaosAction;
import br.com.spring.batch.partitioner.support.chaos.ChaosPoint;
import br.com.spring.batch.partitioner.support.chaos.ChaosRule;
import br.com.spring.batch.partitioner.support.chaos.FailureInjector;

import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Profile("partitioner")
@RequestMapping("/chaos")
public class ChaosController {

    private final FailureInjector failureInjector;

    public ChaosController(FailureInjector failureInjector) {
        this.failureInjector = failureInjector;
    }

    @PutMapping
    public ChaosRule activate(@RequestParam ChaosPoint point,
                              @RequestParam(defaultValue = "FAIL") ChaosAction action,
                              @RequestParam(defaultValue = "1") int onAttempt,
                              @RequestParam(required = false) Integer partitionIndex,
                              @RequestParam(defaultValue = "60") int delaySeconds) {
        ChaosRule rule = new ChaosRule(point, action, onAttempt, partitionIndex, delaySeconds);
        failureInjector.activate(rule);
        return rule;
    }

    @GetMapping
    public Map<String, Object> current() {
        return failureInjector.activeRule()
                .<Map<String, Object>>map(rule -> Map.of("active", true, "rule", rule))
                .orElseGet(() -> Map.of("active", false));
    }

    @DeleteMapping
    public ResponseEntity<Void> deactivate() {
        failureInjector.deactivate();
        return ResponseEntity.noContent().build();
    }
}
