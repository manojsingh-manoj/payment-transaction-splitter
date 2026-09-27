package com.payment.ingestion;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class FileIngestionService {

    private final XmlTransactionSplitterService splitterService;
    private final FileProcessingJobRepository jobRepository;
    private final TransactionMetadataRepository metadataRepository;
    private final ApplicationEventPublisher eventPublisher;

    @Transactional
    public String processFile(Path filePath) {
        String jobId = "JOB-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("Starting ingestion job {} for file: {}", jobId, filePath.getFileName());

        FileProcessingJob job = FileProcessingJob.builder()
                .jobId(jobId)
                .sourceFileName(filePath.getFileName().toString())
                .status("IN_PROGRESS")
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        jobRepository.save(job);

        try {
            List<TransactionMetadata> metadataList = splitterService.splitAndExtractMetadata(filePath, jobId);

            for (TransactionMetadata metadata : metadataList) {
                metadataRepository.save(metadata);

                // Publish internal event via Spring Modulith event bus
                TransactionChunkedEvent event = new TransactionChunkedEvent(
                        metadata.getId(),
                        metadata.getJobId(),
                        metadata.getMsgId(),
                        metadata.getInstructionId(),
                        metadata.getEndToEndId(),
                        metadata.getTransactionId(),
                        metadata.getUetr(),
                        metadata.getAmount(),
                        metadata.getCurrency(),
                        metadata.getSettlementDate(),
                        metadata.getDebtorBic(),
                        metadata.getCreditorBic(),
                        metadata.getChunkedFilePath(),
                        Instant.now()
                );
                eventPublisher.publishEvent(event);
            }

            job.setTotalTransactions(metadataList.size());
            job.setStatus("COMPLETED");
            job.setUpdatedAt(Instant.now());
            jobRepository.save(job);

            log.info("Successfully processed job {}. Extracted and published {} transactions.", jobId, metadataList.size());
            return jobId;

        } catch (Exception e) {
            log.error("Failed to process ingestion job {}", jobId, e);
            job.setStatus("FAILED");
            job.setUpdatedAt(Instant.now());
            jobRepository.save(job);
            throw new RuntimeException("Ingestion failed for job " + jobId, e);
        }
    }
}