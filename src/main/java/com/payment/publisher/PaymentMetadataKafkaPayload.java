package com.payment.publisher;

import java.math.BigDecimal;
import java.time.LocalDate;

public record PaymentMetadataKafkaPayload(
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
        String chunkedFilePath
) {
}