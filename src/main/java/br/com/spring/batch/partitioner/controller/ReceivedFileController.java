package br.com.spring.batch.partitioner.controller;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.enums.FileStatus;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.repository.PartitionFileRepository;
import br.com.spring.batch.partitioner.service.FileStatusService;
import br.com.spring.batch.partitioner.service.FileVerificationService;
import br.com.spring.batch.partitioner.service.FileVerificationService.VerificationReport;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/files")
public class ReceivedFileController {

    private static final int MAX_LIMIT = 500;

    private final OriginalFileRepository originals;
    private final PartitionFileRepository partitions;
    private final FileVerificationService verificationService;
    private final FileStatusService statusService;

    public ReceivedFileController(OriginalFileRepository originals, PartitionFileRepository partitions,
                                  FileVerificationService verificationService, FileStatusService statusService) {
        this.originals = originals;
        this.partitions = partitions;
        this.verificationService = verificationService;
        this.statusService = statusService;
    }

    @GetMapping
    public List<ReceivedFileDocument> list(@RequestParam Optional<FileStatus> status,
                                           @RequestParam(defaultValue = "50") int limit) {
        return originals.findRecent(status, Math.min(limit, MAX_LIMIT));
    }

    @PostMapping("/{id}/requeue")
    public ReceivedFileDocument requeue(@PathVariable String id) {
        return statusService.requeue(id);
    }

    @GetMapping("/summary")
    public Map<FileStatus, Long> summary() {
        return originals.countByStatus();
    }

    @GetMapping("/{id}")
    public ResponseEntity<FileDetail> detail(@PathVariable String id) {
        return originals.findById(id)
                .map(file -> ResponseEntity.ok(new FileDetail(file, partitions.findByParent(id))))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/{id}/verification")
    public ResponseEntity<VerificationReport> verification(@PathVariable String id) {
        return verificationService.verify(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    public record FileDetail(ReceivedFileDocument file, List<ReceivedFileDocument> partitions) {
    }
}
