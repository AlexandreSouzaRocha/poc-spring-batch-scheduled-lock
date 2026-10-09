package br.com.spring.batch.partitioner.batch.tasklet;

import java.util.Optional;

import br.com.spring.batch.partitioner.batch.FilePartitionJob;
import br.com.spring.batch.partitioner.batch.FileStepSupport;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.document.TrailerInfo;
import br.com.spring.batch.partitioner.repository.OriginalFileRepository;
import br.com.spring.batch.partitioner.repository.ProcessingIndicatorRepository;
import br.com.spring.batch.partitioner.support.chaos.ChaosPoint;
import br.com.spring.batch.partitioner.support.log.StructuredLogger;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

@Component
public class FinishFileTasklet implements Tasklet {

    private static final StructuredLogger log = StructuredLogger.of(FinishFileTasklet.class, "partition-job");

    private final FileStepSupport support;
    private final ProcessingIndicatorRepository indicators;
    private final OriginalFileRepository originals;

    public FinishFileTasklet(FileStepSupport support, ProcessingIndicatorRepository indicators,
                             OriginalFileRepository originals) {
        this.support = support;
        this.indicators = indicators;
        this.originals = originals;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        Optional<ReceivedFileDocument> found = support.find(contribution, ChaosPoint.FINISH);
        if (found.isEmpty()) {
            log.info("file.finish").field("fileId", FilePartitionJob.fileIdOf(contribution.getStepExecution()))
                    .field("action", "skip").log("arquivão já removido em tentativa anterior; nada a fazer");
            return RepeatStatus.FINISHED;
        }
        ReceivedFileDocument original = found.get();
        TrailerInfo trailer = original.trailer();
        indicators.upsert(original.movementDate(), original.movementType(), trailer.processingDate(),
                trailer.recordCount());
        originals.deleteById(original.id());
        log.info("file.finish").field("fileId", original.id()).field("movementDate", original.movementDate())
                .field("movementType", original.movementType()).field("processingDate", trailer.processingDate())
                .field("mainframeRecordCount", trailer.recordCount()).data("fileName", original.fileName())
                .log("indicador de processamento atualizado e arquivão removido da gestão de arquivos");
        return RepeatStatus.FINISHED;
    }
}
