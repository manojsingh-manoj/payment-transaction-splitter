# ISO 20022 pacs.009 Payment Transaction Splitter (Spring Modulith POC)

A high-performance, modular monolith application designed to ingest large ISO 20022 `pacs.009` XML payment files, process individual transactions using streaming XML parsing (StAX), persist transaction metadata, and stream records to Apache Kafka via Spring Modulith's transactional outbox event registry.

---

## Technical Stack & Architecture

- **Java Version:** 21
- **Framework:** Spring Boot 3.3.4 & Spring Modulith 1.2.4
- **XML Parsing:** Java StAX (Streaming API for XML) — Ultra-low memory usage for large batch files
- **Database:** PostgreSQL (Hosts domain tables and Spring Modulith Event Publication Registry)
- **Messaging:** Apache Kafka (KRaft mode)
- **Build Tool:** Apache Maven

### Modular Boundaries & Package Structure

```text
com.payment
├── PaymentApplication.java
├── ingestion
│   ├── package-info.java
│   ├── FileProcessingJob.java
│   ├── FileProcessingJobRepository.java
│   ├── TransactionMetadata.java
│   ├── TransactionMetadataRepository.java
│   ├── XmlTransactionSplitterService.java
│   ├── FileIngestionService.java
│   ├── TransactionChunkedEvent.java
│   └── IngestionController.java
└── publisher
    ├── package-info.java
    ├── PaymentMetadataKafkaPayload.java
    └── TransactionMetadataPublisher.java

```
# Payment Application

## Technical Description

This application implements a decoupled, event-driven pipeline tailored for high-throughput ISO 20022 XML parsing without sacrificing transactional integrity or domain isolation.

### Key Architectural Highlights

* **Memory-Efficient StAX Parsing:** Uses StAX (Streaming API for XML) to read incoming `pacs.009` files node-by-node. It maintains an **O(1) constant memory footprint**, allowing it to process multi-gigabyte XML files without triggering OutOfMemory (OOM) errors.

* **Domain Isolation with Spring Modulith:** Enforces strict boundary rules between the `ingestion` context (file parsing and metadata persistence) and the `publisher` context (Kafka message serialization).

* **Guaranteed Delivery (Transactional Outbox):** Leverages Spring Modulith's persistent `event_publication` registry in PostgreSQL. Internal events (`TransactionChunkedEvent`) are written to the database in the exact same database transaction as the business metadata. If Kafka experiences downtime, events are safely retained in PostgreSQL and retried automatically, ensuring zero data loss and **at-least-once delivery** to downstream systems.

## 1. Prerequisites

Before running the application, make sure you have the following installed:

* **Java Development Kit (JDK):** Version 21 or higher
* **Container Runtime:** Docker Desktop or Docker Engine with Docker Compose

## 2. Infrastructure Setup

Start the local PostgreSQL and Kafka (KRaft mode) containers:

```bash
docker compose up -d
```

Verify that both containers are up and running:

```bash
docker compose ps
```

## 3. Build & Run Application

You can launch the application using the Maven wrapper.

### Start the Application

```bash
./mvnw spring-boot:run
```

### Trigger File Ingestion

Execute the following endpoint to process the sample XML file:

```bash
curl -X POST "http://localhost:8080/api/v1/ingestion/process?fileName=sample-pacs009-batch.xml"
```

### Verify Output XML Files

Check the generated output files:

```bash
ls -la data/output/
```

### Verify Kafka Events

Consume messages from the `payment.transaction.metadata` Kafka topic:

```bash
docker exec -it payment-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic payment.transaction.metadata \
  --from-beginning
```

### Verify PostgreSQL Persistent Events

Inspect the Spring Modulith event publication table inside the PostgreSQL container:

```bash
docker exec -it payment-postgres \
  psql -U postgres -d payment_db \
  -c "SELECT id, event_type, publication_date, completion_date FROM event_publication;"
```
