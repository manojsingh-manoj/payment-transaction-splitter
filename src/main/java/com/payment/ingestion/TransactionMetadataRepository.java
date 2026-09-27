package com.payment.ingestion;

import org.springframework.data.jpa.repository.JpaRepository;

public interface TransactionMetadataRepository extends JpaRepository<TransactionMetadata, String> {
}