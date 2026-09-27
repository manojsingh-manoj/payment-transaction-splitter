package com.payment.ingestion;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "file_processing_job")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileProcessingJob {

    @Id
    private String jobId;
    private String sourceFileName;
    private int totalTransactions;
    private String status; // IN_PROGRESS, COMPLETED, FAILED
    private Instant createdAt;
    private Instant updatedAt;
}