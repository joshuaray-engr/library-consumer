# Product Requirement Document (PRD)
## `library-consumer` — Kafka-to-Dual-Persistence Microservice

| Field | Value |
|---|---|
| **Service Name** | `library-consumer` |
| **Owner** | Platform Engineering |
| **Status** | Draft v1.0 |
| **Runtime** | Java 25 (LTS) |
| **Framework** | Spring Boot 4.x |
| **Cloud Provider** | AWS |

---

## 1. Executive Summary & Objectives

### 1.1 High-Level Goal
`library-consumer` is an event-driven microservice that subscribes to a `library-events` topic on an AWS-hosted Apache Kafka cluster. For every inbound `LibraryEvent` message, the service performs a **dual-write fan-out**:

1. **Flattens** the nested JSON payload and persists a denormalized record to **AWS DynamoDB** for high-throughput, low-latency lookups.
2. **Normalizes** the same payload into a relational `books` schema, persisted to **AWS RDS PostgreSQL** via JPA, with schema lifecycle managed by **Flyway**.

The service acts as the system-of-record bridge between an append-only event stream and two purpose-built storage engines (NoSQL for read-speed, RDBMS for relational integrity/reporting).

### 1.2 Core Tech Stack Enforcement

| Layer | Technology | Notes |
|---|---|---|
| Language | **Java 25** | Must use **Virtual Threads** (`Thread.ofVirtual()` / `spring.threads.virtual.enabled=true`) for all Kafka listener container and I/O-bound execution. Use modern **Records** for all DTOs — no Lombok POJOs for data carriers. |
| Framework | **Spring Boot 4.0.x** | Baseline `spring-boot-starter-parent`. |
| Messaging | **Apache Kafka** (AWS self-managed EC2 cluster) | `spring-kafka` client. |
| NoSQL | **AWS DynamoDB** | AWS SDK v2 **Enhanced Client** (`software.amazon.awssdk:dynamodb-enhanced`). |
| RDBMS | **AWS RDS PostgreSQL** | `spring-boot-starter-data-jpa` + `postgresql` driver. |
| Schema Migration | **Flyway** | `flyway-core` + `flyway-database-postgresql`. |
| Build | Maven or Gradle (Java 25 toolchain) | `--enable-preview` only if leveraging preview features; otherwise standard GA features. |

### 1.3 Objectives Checklist
- ✅ Consume Kafka events reliably with at-least-once semantics.
- ✅ Flatten & persist to DynamoDB idempotently.
- ✅ Persist/upsert relational book entity via JPA with Flyway-managed DDL.
- ✅ Expose a synchronous REST inspection API for operational visibility.
- ✅ Guarantee idempotency and DLQ fallback on partial failure.

---

## 2. Inbound Data Contract (Kafka)

### 2.1 JSON Schema (Wire Format)

```json
{
  "libraryEventId": 456,
  "book": {
    "bookId": 123,
    "bookName": "The Great Gatsby - Updated",
    "bookAuthor": "F. Scott Fitzgerald"
  }
}
```

**Schema Definition Table**

| Field | Path | Type (JSON) | Type (Java) | Required |
|---|---|---|---|---|
| `libraryEventId` | `$.libraryEventId` | Number | `Long` | Yes |
| `book.bookId` | `$.book.bookId` | Number | `Long` | Yes |
| `book.bookName` | `$.book.bookName` | String | `String` | Yes |
| `book.bookAuthor` | `$.book.bookAuthor` | String | `String` | Yes |

### 2.2 Infrastructure Connection Specification

```yaml
spring:
  kafka:
    bootstrap-servers:
      - ec2-18-223-60-27.us-east-2.compute.amazonaws.com:9092
      - ec2-18-223-60-27.us-east-2.compute.amazonaws.com:9093
      - ec2-18-223-60-27.us-east-2.compute.amazonaws.com:9094
    consumer:
      group-id: library-consumer-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      properties:
        spring.json.trusted.packages: "com.library.consumer.dto"
    listener:
      ack-mode: manual_immediate
      concurrency: 3   # matches partition count
```

| Parameter | Value |
|---|---|
| **Host** | `ec2-18-223-60-27.us-east-2.compute.amazonaws.com` |
| **Broker Ports** | `9092`, `9093`, `9094` (multi-broker, one listener per port) |
| **Topic Name** | `library-events` |
| **Partitions** | `3` |
| **Consumer Group ID** | `library-consumer-group` |
| **Deserialization** | Raw `String` → manual `ObjectMapper.readValue()` into `LibraryEventRecord` (avoids strict type-header coupling from producer) |

> **Note:** Partition count (3) directly maps to `listener.concurrency: 3` to ensure one virtual-thread-backed consumer thread per partition, maximizing parallel ingestion without over-subscribing brokers.

---

## 3. Dual-Persistence Functional Requirements

### 3.A NoSQL Path — AWS DynamoDB

**Objective:** On message receipt, flatten the nested `book` object into a single-level DynamoDB item keyed by `libraryEventId`.

**Table Definition**

| Property | Value |
|---|---|
| Table Name | `LibraryEvents` |
| Partition Key | `libraryEventId` (Type: `N`) |
| Billing Mode | `PAY_PER_REQUEST` (on-demand) |
| Region | `us-east-2` |

**Exact Attribute Mapping (Flattened Item)**

| Source (JSON path) | DynamoDB Attribute | DynamoDB Type | Example Raw Wire Value |
|---|---|---|---|
| `libraryEventId` | `libraryEventId` | `N` (Number) | `{"N": "456"}` |
| `book.bookId` | `bookId` | `N` (Number) | `{"N": "123"}` |
| `book.bookName` | `bookName` | `S` (String) | `{"S": "The Great Gatsby - Updated"}` |
| `book.bookAuthor` | `bookAuthor` | `S` (String) | `{"S": "F. Scott Fitzgerald"}` |

**Resulting Flattened Item (PutItem payload):**

```json
{
  "libraryEventId": {"N": "456"},
  "bookId": {"N": "123"},
  "bookName": {"S": "The Great Gatsby - Updated"},
  "bookAuthor": {"S": "F. Scott Fitzgerald"}
}
```

**Write Semantics:** Use `PutItem` (full overwrite) keyed on `libraryEventId` — this makes the write **naturally idempotent**; replays of the same `libraryEventId` simply overwrite the same item with identical (or updated) data.

---

### 3.B Relational Path — AWS RDS PostgreSQL (JPA + Flyway)

**Objective:** Maintain a normalized, queryable `books` table representing the relational system-of-record, independent of the Kafka envelope (`libraryEventId` is NOT part of the relational schema — it is an event-transport artifact only).

**Target Table:** `books`

**Entity Mapping**

| Column | Type | Constraint |
|---|---|---|
| `book_id` | `BIGINT` | `PRIMARY KEY` |
| `book_name` | `VARCHAR(255)` | `NOT NULL` |
| `book_author` | `VARCHAR(255)` | `NOT NULL` |
| `created_at` | `TIMESTAMP` | `DEFAULT now()` |
| `updated_at` | `TIMESTAMP` | `DEFAULT now()` |

**Flyway Migration File:** `src/main/resources/db/migration/V1__init_books_table.sql`

```sql
-- V1__init_books_table.sql
-- Initial schema for the relational "books" system-of-record.

CREATE TABLE IF NOT EXISTS books (
    book_id      BIGINT       PRIMARY KEY,
    book_name    VARCHAR(255) NOT NULL,
    book_author  VARCHAR(255) NOT NULL,
    created_at   TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_books_book_author ON books (book_author);
```

**Flyway Configuration**

```yaml
spring:
  flyway:
    enabled: true
    locations: classpath:db/migration
    baseline-on-migrate: true
    validate-on-migrate: true
  datasource:
    url: jdbc:postgresql://<rds-endpoint>:5432/librarydb
    username: ${DB_USERNAME}
    password: ${DB_SECRET}   # sourced from AWS Secrets Manager
  jpa:
    hibernate:
      ddl-auto: validate   # Flyway owns schema; Hibernate must never auto-generate DDL
    properties:
      hibernate.dialect: org.hibernate.dialect.PostgreSQLDialect
```

**Write Semantics:** Use `saveAndFlush` with an **upsert-by-primary-key** strategy — `JpaRepository.save()` on an entity with an existing `book_id` performs a `MERGE`/`UPDATE` (Hibernate dirty-checking), ensuring idempotent handling of duplicate/replayed events on `bookId`.

---

## 4. REST Endpoint Specifications

**Purpose:** Provide a synchronous, read-only inspection surface for operators/QA to poll last-processed events and verify pipeline health without needing direct DB/DynamoDB console access.

### 4.1 `GET /api/v1/library-events/latest`

Returns the most recently processed library event as persisted across both stores (reconciled view).

**Response `200 OK`:**
```json
{
  "libraryEventId": 456,
  "book": {
    "bookId": 123,
    "bookName": "The Great Gatsby - Updated",
    "bookAuthor": "F. Scott Fitzgerald"
  },
  "dynamoDbStatus": "PERSISTED",
  "postgresStatus": "PERSISTED",
  "processedAt": "2026-09-01T11:53:14-05:00"
}
```

### 4.2 `GET /api/v1/library-events/{libraryEventId}`

Fetch a specific event's flattened DynamoDB item plus the linked relational `books` row, by `libraryEventId`.

| Status | Condition |
|---|---|
| `200 OK` | Event found in at least one store |
| `404 Not Found` | No record in either store |

### 4.3 `GET /actuator/health` (Management Endpoint)

Standard Spring Boot Actuator health check, extended with custom `HealthIndicator` beans:
- `kafkaConsumerHealthIndicator` — verifies listener container is running (not paused/dead).
- `dynamoDbHealthIndicator` — `DescribeTable` ping.
- `postgresHealthIndicator` — standard DataSource health (built-in).

---

## 5. Architectural Blueprint & Component Design

### 5.1 Package Layout

```
com.library.consumer
├── LibraryConsumerApplication.java
├── config
│   ├── KafkaConsumerConfig.java
│   ├── DynamoDbConfig.java
│   ├── VirtualThreadConfig.java
│   └── FlywayConfig.java
├── dto
│   ├── LibraryEventRecord.java      (record)
│   └── BookRecord.java              (record)
├── consumer
│   └── LibraryEventKafkaListener.java
├── service
│   ├── LibraryEventProcessorService.java
│   └── LibraryEventReconciliationService.java
├── repository
│   ├── DynamoDbRepository.java
│   └── BookJpaRepository.java
├── entity
│   └── BookEntity.java
├── controller
│   └── LibraryEventController.java
├── exception
│   ├── DynamoDbPersistenceException.java
│   ├── PostgresPersistenceException.java
│   └── NonRetryableEventException.java
└── errorhandler
    └── LibraryEventDlqPublisher.java
```

### 5.2 DTOs — Java 25 Records

```java
// dto/BookRecord.java
public record BookRecord(
        Long bookId,
        String bookName,
        String bookAuthor
) {}
```

```java
// dto/LibraryEventRecord.java
public record LibraryEventRecord(
        Long libraryEventId,
        BookRecord book
) {}
```

### 5.3 Kafka Consumer Component

```java
// consumer/LibraryEventKafkaListener.java
@Component
public class LibraryEventKafkaListener {

    private final LibraryEventProcessorService processorService;
    private final ObjectMapper objectMapper;

    public LibraryEventKafkaListener(LibraryEventProcessorService processorService,
                                      ObjectMapper objectMapper) {
        this.processorService = processorService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
        topics = "library-events",
        groupId = "library-consumer-group",
        concurrency = "3"
    )
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        LibraryEventRecord event = objectMapper.readValue(record.value(), LibraryEventRecord.class);
        processorService.process(event);
        ack.acknowledge();
    }
}
```

### 5.4 Repositories

```java
// repository/DynamoDbRepository.java
@Repository
public class DynamoDbRepository {

    private final DynamoDbTable<LibraryEventItem> table; // Enhanced Client mapped bean

    public void save(LibraryEventRecord event) {
        LibraryEventItem item = LibraryEventItem.from(event); // flattening logic here
        table.putItem(item);
    }

    public Optional<LibraryEventItem> findById(Long libraryEventId) {
        return Optional.ofNullable(
            table.getItem(Key.builder().partitionValue(libraryEventId).build())
        );
    }
}
```

```java
// repository/BookJpaRepository.java
public interface BookJpaRepository extends JpaRepository<BookEntity, Long> {
    Optional<BookEntity> findByBookId(Long bookId);
}
```

### 5.5 Coordinating Service

```java
// service/LibraryEventProcessorService.java
@Service
public class LibraryEventProcessorService {

    private final DynamoDbRepository dynamoDbRepository;
    private final BookJpaRepository bookJpaRepository;
    private final LibraryEventDlqPublisher dlqPublisher;

    public void process(LibraryEventRecord event) {
        try {
            dynamoDbRepository.save(event);          // Step 1: NoSQL flatten-write
            persistRelational(event);                 // Step 2: JPA transactional upsert
        } catch (DynamoDbPersistenceException | PostgresPersistenceException ex) {
            dlqPublisher.publish(event, ex);           // Structured fallback to DLQ
        }
    }

    @Transactional
    void persistRelational(LibraryEventRecord event) {
        BookRecord b = event.book();
        BookEntity entity = bookJpaRepository.findByBookId(b.bookId())
            .map(existing -> existing.update(b.bookName(), b.bookAuthor()))
            .orElseGet(() -> BookEntity.from(b));
        bookJpaRepository.save(entity);
    }
}
```

> **Transaction Boundary Note:** DynamoDB and PostgreSQL cannot share a single ACID transaction. The service uses a **Saga-style compensating flow**: DynamoDB write is attempted first (cheap, idempotent overwrite); PostgreSQL write is wrapped in `@Transactional`. If either fails, the event is routed to the DLQ for reprocessing rather than partially silently dropped.

---

## 6. Non-Functional Requirements & Error Handling

### 6.1 Dead Letter Queue (DLQ) Strategy

| Failure Scenario | Behavior |
|---|---|
| DynamoDB write fails (throttling, `ProvisionedThroughputExceededException`) | Retry with exponential backoff (`RetryTemplate`, max 3 attempts) → on exhaustion, publish to `library-events.DLT` |
| PostgreSQL write fails (constraint violation, connection loss) | Roll back JPA transaction → publish full original payload + error metadata to `library-events.DLT` |
| Deserialization failure (malformed JSON) | Route directly to `library-events.DLT` without retry (non-retryable) |

**DLQ Topic:** `library-events.DLT`
**DLQ Publisher:** Uses `DeadLetterPublishingRecoverer` (Spring Kafka) wired into a `DefaultErrorHandler` with `FixedBackOff(1000L, 3)`.

```java
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
    var recoverer = new DeadLetterPublishingRecoverer(template,
        (record, ex) -> new TopicPartition("library-events.DLT", record.partition()));
    return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
}
```

### 6.2 Idempotency Guarantees

| Store | Idempotency Mechanism |
|---|---|
| **DynamoDB** | `PutItem` keyed on `libraryEventId` (partition key) — replays overwrite identically, no duplicates possible by design. |
| **PostgreSQL** | `findByBookId` pre-check + upsert on `book_id` primary key — duplicate Kafka deliveries resolve to the same row (`INSERT ... ON CONFLICT` semantics via JPA merge). |
| **Consumer Offset** | Manual acknowledgment (`ack-mode: manual_immediate`) — offset committed **only after both writes succeed**, guaranteeing at-least-once delivery with safe re-processing on crash/restart. |

### 6.3 Additional Non-Functional Requirements

- **Concurrency:** Virtual threads (`spring.threads.virtual.enabled=true`) back all `@KafkaListener` container threads and JDBC-bound service calls to maximize throughput under blocking I/O without platform-thread exhaustion.
- **Observability:** Micrometer + Actuator metrics for `kafka.consumer.lag`, `dynamodb.put.latency`, `postgres.write.errors`.
- **Security:** DB credentials and AWS access via IAM roles / Secrets Manager — no hardcoded secrets in `application.yml`.
- **Resilience:** Circuit breaker (Resilience4j) around DynamoDB/RDS calls to prevent cascading failure during AWS-side throttling events.
