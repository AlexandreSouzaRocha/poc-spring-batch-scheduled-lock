package br.com.spring.batch.partitioner.controller;

import br.com.spring.batch.partitioner.model.document.ProcessingIndicatorDocument;
import br.com.spring.batch.partitioner.repository.ProcessingIndicatorRepository;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/indicators")
public class ProcessingIndicatorController {

    private final ProcessingIndicatorRepository indicators;

    public ProcessingIndicatorController(ProcessingIndicatorRepository indicators) {
        this.indicators = indicators;
    }

    @GetMapping("/{movementDate}")
    public ResponseEntity<ProcessingIndicatorDocument> indicator(@PathVariable String movementDate) {
        return ResponseEntity.of(indicators.findById(movementDate));
    }
}
