package com.payment.ingestion;

import org.springframework.data.jpa.repository.JpaRepository;

public interface FileProcessingJobRepository extends JpaRepository<FileProcessingJob, String> {
}