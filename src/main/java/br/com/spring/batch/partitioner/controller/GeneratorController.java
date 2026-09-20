package br.com.spring.batch.partitioner.controller;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import br.com.spring.batch.partitioner.model.enums.MovementType;
import br.com.spring.batch.partitioner.model.layout.FileLayout;
import br.com.spring.batch.partitioner.service.generation.FileGeneratorService;
import br.com.spring.batch.partitioner.service.generation.GeneratedFile;
import br.com.spring.batch.partitioner.service.generation.GenerationRequest;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import org.springframework.context.annotation.Profile;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Validated
@Profile("generator")
@RequestMapping("/generator")
public class GeneratorController {

    private final FileGeneratorService generatorService;
    private final FileLayout layout;

    public GeneratorController(FileGeneratorService generatorService, FileLayout layout) {
        this.generatorService = generatorService;
        this.layout = layout;
    }

    @PostMapping("/files")
    public List<GeneratedFile> generate(
            @RequestParam(defaultValue = "1000000") @Min(1) long lines,
            @RequestParam(defaultValue = "ABERTO") MovementType movementType,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate movementDate,
            @RequestParam(defaultValue = "1") @Min(1) @Max(20) int files,
            @RequestParam(defaultValue = "false") boolean invalidHeader) {
        LocalDate date = Optional.ofNullable(movementDate).orElseGet(LocalDate::now);
        return generatorService.generate(new GenerationRequest(lines, movementType, date, files, invalidHeader,
                layout));
    }
}
