package com.payment.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payment.ingestion.TransactionChunkedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionMetadataPublisher {

    private static final String KAFKA_TOPIC = "payment.transaction.metadata";
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @ApplicationModuleListener
    public void onTransactionChunked(TransactionChunkedEvent event) {
        log.info("Received TransactionChunkedEvent for UETR: {}", event.uetr());

        try {
            PaymentMetadataKafkaPayload payload = new PaymentMetadataKafkaPayload(
                    event.metadataId(),
                    event.jobId(),
                    event.msgId(),
                    event.instructionId(),
                    event.endToEndId(),
                    event.transactionId(),
                    event.uetr(),
                    event.amount(),
                    event.currency(),
                    event.settlementDate(),
                    event.debtorBic(),
                    event.creditorBic(),
                    event.chunkedFilePath()
            );

            String jsonPayload = objectMapper.writeValueAsString(payload);

            // Synchronous send/wait to guarantee Kafka ack before completing Modulith event publication
            kafkaTemplate.send(KAFKA_TOPIC, event.uetr(), jsonPayload).get();

            log.info("Successfully published metadata for UETR: {} to Kafka topic {}", event.uetr(), KAFKA_TOPIC);

        } catch (Exception e) {
            log.error("Failed to publish metadata for UETR: {} to Kafka", event.uetr(), e);
            throw new RuntimeException("Kafka publishing failed", e);
        }
    }
}