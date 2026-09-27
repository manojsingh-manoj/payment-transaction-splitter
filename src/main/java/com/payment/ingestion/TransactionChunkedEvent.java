package com.payment.ingestion;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

public record TransactionChunkedEvent(
        String metadataId,
        String jobId,
        String msgId,
        String instructionId,
        String endToEndId,
        String transactionId,
        String uetr,
        BigDecimal amount,
        String currency,
        LocalDate settlementDate,
        String debtorBic,
        String creditorBic,
        String chunkedFilePath,
        Instant timestamp
) {
}