package com.payment.ingestion;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

@Entity
@Table(name = "transaction_metadata")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionMetadata {

    @Id
    private String id;
    private String jobId;
    private String msgId;
    private String instructionId;
    private String endToEndId;
    private String transactionId;
    private String uetr;
    private BigDecimal amount;
    private String currency;
    private LocalDate settlementDate;
    private String debtorBic;
    private String creditorBic;
    private String chunkedFilePath;
    private Instant createdAt;
}