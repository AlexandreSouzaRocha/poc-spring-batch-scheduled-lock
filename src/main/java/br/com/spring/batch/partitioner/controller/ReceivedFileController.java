package br.com.spring.batch.partitioner.controller;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.enums.FileStatus;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.service.FileQueryService;
import br.com.spring.batch.partitioner.service.FileQueryService.FileDetail;
import br.com.spring.batch.partitioner.service.FileStatusService;
import br.com.spring.batch.partitioner.service.FileVerificationService;
import br.com.spring.batch.partitioner.service.FileVerificationService.VerificationReport;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/files")
public class ReceivedFileController {

    private static final int MAX_LIMIT = 500;

    private final OriginalFileRepository originals;
    private final FileQueryService queryService;
    private final FileVerificationService verificationService;
    private final FileStatusService statusService;

    public ReceivedFileController(OriginalFileRepository originals, FileQueryService queryService,
                                  FileVerificationService verificationService, FileStatusService statusService) {
        this.originals = originals;
        this.queryService = queryService;
        this.verificationService = verificationService;
        this.statusService = statusService;
    }

    @GetMapping
    public List<ReceivedFileDocument> list(@RequestParam Optional<String> status,
                                           @RequestParam(defaultValue = "50") int limit) {
        return originals.findRecent(status.map(FileStatus::fromValue), Math.min(limit, MAX_LIMIT));
    }

    @PostMapping("/{id}/requeue")
    public ReceivedFileDocument requeue(@PathVariable String id) {
        return statusService.requeue(id);
    }

    @GetMapping("/summary")
    public Map<String, Long> summary() {
        Map<String, Long> summary = new LinkedHashMap<>();
        originals.countByStatus().forEach((status, count) -> summary.put(status.value(), count));
        return summary;
    }

    @GetMapping("/{id}")
    public ResponseEntity<FileDetail> detail(@PathVariable String id) {
        return ResponseEntity.of(queryService.detail(id));
    }

    @GetMapping("/{id}/verification")
    public ResponseEntity<VerificationReport> verification(@PathVariable String id) {
        return ResponseEntity.of(verificationService.verify(id));
    }
}
