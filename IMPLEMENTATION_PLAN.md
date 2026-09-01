# Implementation Plan — `library-consumer`

Strict, phase-by-phase implementation plan for the Kafka consumer application. No application code is generated until each phase's verification steps pass and explicit confirmation is given to proceed.

## Codebase Baseline (as found)

| Item | Current State |
|---|---|
| Root package | `com.kafkaplayground` → **to be renamed to `com.library.consumer`** (decision confirmed) |
| `build.gradle` | Spring Boot 4.1.1, Java 25 toolchain. Has `spring-boot-starter-kafka`, `spring-boot-starter-data-jpa`, `spring-boot-starter-flyway`, `postgresql` driver, Lombok, validation, webmvc. **Missing:** AWS SDK v2 (`dynamodb-enhanced`), Testcontainers, `spring-kafka-test`. Several artifact IDs are non-canonical for Spring Boot 4 and **must be corrected** (e.g. `spring-boot-starter-webmvc`, `spring-boot-starter-flyway`, and the matching `*-test` variants). |
| `src/main` | Only `LibraryConsumerApplication.java` (bootstrap class) + empty `application.properties`. No consumer, no DTOs, no config classes. |
| `src/test` | Only the default `LibraryConsumerApplicationTests.java` context-load smoke test. |
| Flyway migrations | None exist yet (`src/main/resources/db/migration` folder absent). |
| `application.properties` | Empty aside from app name — no Kafka bootstrap servers, no datasource config. |

This confirms we are starting from a clean scaffold — nothing to remove, only to add, phase by phase.

**Pre-Phase-1 setup (confirmed decisions):**
1. Rename package `com.kafkaplayground` → `com.library.consumer` (move existing files, update package declarations/imports).
2. Correct `build.gradle` artifact IDs to real Spring Boot 4 starter names, and add `spring-kafka-test`.

---

## PHASE 1 — Kafka Ingestion & Logging (No Database Writing)

**Objective:** Prove a stable, testable connection to the `library-events` topic that deserializes messages into typed records and logs them. No persistence of any kind.

**Files to create:**

| File | Purpose |
|---|---|
| `src/main/java/com/library/consumer/dto/BookRecord.java` | Java 25 `record` — `bookId`, `bookName`, `bookAuthor` |
| `src/main/java/com/library/consumer/dto/LibraryEventRecord.java` | Java 25 `record` — `libraryEventId`, `book` |
| `src/main/java/com/library/consumer/config/KafkaConsumerConfig.java` | `ConsumerFactory`/`ConcurrentKafkaListenerContainerFactory` beans, manual-ack mode, error handler stub (log-only, no DLQ yet) |
| `src/main/java/com/library/consumer/consumer/LibraryEventKafkaListener.java` | `@KafkaListener` on `library-events`, deserializes JSON → `LibraryEventRecord`, logs at INFO, acknowledges |
| `src/main/resources/application.yml` (replace/extend `application.properties`) | `spring.kafka.bootstrap-servers` (3 brokers), `group-id: library-consumer-group`, `concurrency: 3`, `ack-mode: manual_immediate`, logging pattern config |
| `src/main/resources/logback-spring.xml` (optional) | Structured log pattern including `libraryEventId` for traceability |

**Files to modify:**
- `build.gradle`: add `testImplementation 'org.springframework.kafka:spring-kafka-test'`.

**Constraints enforced:**
- No `@Entity`, no `JpaRepository`, no `DynamoDbTable` references anywhere in this phase's diff.
- No datasource/Flyway properties activated (leave `spring.datasource.*` unset so context doesn't attempt a real DB connection during tests — use slice tests instead of full context where needed).

**Verification steps required before Phase 2:**
1. **Unit tests** for `LibraryEventKafkaListener` — mock the deserialized `ConsumerRecord`/`Acknowledgment`, assert the listener logs and calls `ack.acknowledge()` exactly once, and that malformed JSON is handled without crashing the thread.
2. **Unit tests** for JSON mapping — round-trip `ObjectMapper` serialize/deserialize of `LibraryEventRecord` against the exact PRD wire-format JSON.
3. **Embedded Kafka smoke test** using `@EmbeddedKafka` (from `spring-kafka-test`) — publish a real message to an in-memory broker/topic and assert the listener receives and logs it (no assertions on any DB).
4. `./gradlew test` passes with 100% of new tests green; no DB-related beans present in the Spring context for these tests (verify by checking no `DataSource`/`EntityManagerFactory` bean is required to start the test context).
5. Manual/local run against the real AWS Kafka cluster (`ec2-18-223-60-27...`) confirming console logs show consumed messages — **before moving to Phase 2**.

---

## PHASE 2 — DynamoDB Integration (Write-Only)

**Objective:** Persist the flattened `LibraryEventRecord` into DynamoDB. PostgreSQL remains completely untouched.

**Files to create:**

| File | Purpose |
|---|---|
| `src/main/java/com/library/consumer/entity/dynamo/LibraryEventItem.java` | `@DynamoDbBean`-annotated flattened item: `libraryEventId` (PK), `bookId`, `bookName`, `bookAuthor` |
| `src/main/java/com/library/consumer/config/DynamoDbConfig.java` | `DynamoDbClient` + `DynamoDbEnhancedClient` beans, region/endpoint override support (for local testing) |
| `src/main/java/com/library/consumer/repository/DynamoDbRepository.java` | `save(LibraryEventRecord)`, `findById(Long)` using Enhanced Client `putItem`/`getItem` |
| `src/main/java/com/library/consumer/mapper/LibraryEventItemMapper.java` (or static factory on `LibraryEventItem`) | Flattening logic: `LibraryEventRecord` → `LibraryEventItem` |

**Files to modify:**
- `build.gradle`: add `software.amazon.awssdk:dynamodb-enhanced`, and for tests either `com.amazonaws:DynamoDBLocal` or a Testcontainers `localstack`/`dynamodb-local` module (recommend Testcontainers to keep consistency with Phase 4).
- `LibraryEventKafkaListener.java`: inject `DynamoDbRepository`, call `.save(event)` after successful deserialization, wrap in try/catch that logs on failure (full DLQ routing deferred to a dedicated NFR phase after Phase 4).
- `application.yml`: add `aws.dynamodb.table-name`, `aws.region`, optional `aws.dynamodb.endpoint` (for local override in tests).

**Constraints enforced:**
- Zero `spring.datasource.*`, zero `@Entity`/JPA repository code introduced.
- `application.yml` still must not require a live PostgreSQL connection to start.

**Verification steps required before Phase 3:**
1. **Unit tests** for `LibraryEventItemMapper`/flattening logic — assert exact attribute mapping matches PRD spec (Number vs. String types), via the constructed `LibraryEventItem` fields.
2. **Unit tests** for `DynamoDbRepository` — mock `DynamoDbTable`/`DynamoDbEnhancedClient` (Mockito) to verify `putItem` invoked with correctly-shaped item; verify `findById` builds the correct `Key`.
3. **Repository-level test against DynamoDB Local (or Testcontainers `localstack`/`amazon/dynamodb-local` image)** — spin up real local table, `putItem`, `getItem`, assert round-trip equality including type correctness (Number vs String).
4. **Updated consumer test** — extend Phase 1's `@EmbeddedKafka` test to assert the DynamoDB repository `save()` is invoked (mocked) after message consumption.
5. Confirm idempotency: run the same `libraryEventId` twice through the repository test and assert the item is overwritten, not duplicated (single item count in table).
6. `./gradlew test` green; no PostgreSQL/JPA beans touched or required.

---

## PHASE 3 — PostgreSQL `books` Table Integration (Update-Only)

**Objective:** Add relational persistence via JPA + Flyway, updating/upserting the `books` table per PRD schema. DynamoDB logic untouched.

**Files to create:**

| File | Purpose |
|---|---|
| `src/main/resources/db/migration/V1__init_books_table.sql` | Flyway migration exactly per PRD schema (`book_id BIGINT PK`, `book_name`, `book_author`, timestamps, index) |
| `src/main/java/com/library/consumer/entity/BookEntity.java` | `@Entity` mapped to `books`, includes `update(bookName, bookAuthor)` convenience method |
| `src/main/java/com/library/consumer/repository/BookJpaRepository.java` | `extends JpaRepository<BookEntity, Long>`, `findByBookId` (or use `bookId` as `@Id` directly and use `findById`) |
| `src/main/java/com/library/consumer/service/LibraryEventProcessorService.java` | Coordinating service: orchestrates DynamoDB save + Postgres upsert in one place, moves logic out of the Kafka listener (listener becomes thin) |

**Files to modify:**
- `LibraryEventKafkaListener.java`: replace direct `DynamoDbRepository` call with a single call to `LibraryEventProcessorService.process(event)`.
- `build.gradle`: confirm/finalize `spring-boot-starter-data-jpa` + `flyway-database-postgresql` dependency correctness (already corrected in pre-Phase-1 setup).
- `application.yml`: add `spring.datasource.*`, `spring.flyway.*`, `spring.jpa.hibernate.ddl-auto=validate`.

**Constraints enforced:**
- DynamoDB write logic/paths remain unchanged — only additive service-layer wiring.
- No new business rules beyond: find-or-create by `bookId`, update `bookName`/`bookAuthor`, persist.

**Verification steps required before Phase 4:**
1. **Flyway migration test** — against a test Postgres instance (Testcontainers or local) confirming `V1__init_books_table.sql` applies cleanly and schema matches spec (columns, types, PK, index).
2. **Unit tests** for `BookEntity` update logic (pure POJO assertions, no Spring context needed).
3. **Unit tests** for `BookJpaRepository` usage inside `LibraryEventProcessorService` — mock `BookJpaRepository`, verify `save()` called with correct upsert semantics (existing row updated vs. new row created), using Mockito, no real DB.
4. **Repository integration test** against a real/local Postgres (Testcontainers) — insert, then reprocess same `bookId` with updated fields, assert single row with updated values (idempotency proof).
5. **Updated consumer/service test** — verify `LibraryEventProcessorService.process()` invokes both `DynamoDbRepository.save()` and `BookJpaRepository.save()` (mocked) exactly once each per message.
6. `./gradlew test` green across all previous + new tests.

---

## PHASE 4 — Integration & Component Testing (No New Business Logic)

**Objective:** End-to-end confidence across the full pipeline using real (containerized) infrastructure. No functional code changes — test code and minimal test-support config only.

**Files to create:**

| File | Purpose |
|---|---|
| `src/test/java/com/library/consumer/integration/LibraryEventPipelineIT.java` | Full E2E: Testcontainers for Kafka + Postgres + DynamoDB-local/localstack; publish raw JSON to topic, assert row in Postgres AND item in DynamoDB |
| `src/test/resources/application-it.yml` | Test-profile config pointing at container-provided endpoints (dynamic ports via `@DynamicPropertySource`) |
| `src/test/java/com/library/consumer/integration/support/TestContainersConfig.java` | Shared container lifecycle (Kafka, Postgres, DynamoDB-local) reusable across IT classes |
| `src/test/java/com/library/consumer/integration/DlqRoutingIT.java` (if DLQ implemented) | Verify malformed/failing messages land on `library-events.DLT` |

**Files modified:**
- `build.gradle`: add `org.testcontainers:junit-jupiter`, `org.testcontainers:kafka`, `org.testcontainers:postgresql`, and a DynamoDB-local/localstack Testcontainers module.

**Constraints enforced:**
- Zero new `@Service`/`@Repository`/`@Component` production classes — only test infrastructure and test classes.
- If a gap is found requiring production code change, that fix must be flagged and routed back to the relevant earlier phase (1–3), not patched ad hoc in Phase 4.

**Verification steps (this phase's tests ARE the exit criteria for the whole project):**
1. Full pipeline test: produce raw Kafka JSON → consumed → DynamoDB item present with exact attribute shape → Postgres `books` row present/updated.
2. Duplicate-message replay test: publish same `libraryEventId`/`bookId` twice → assert no duplicate DynamoDB items, no duplicate Postgres rows (idempotency end-to-end).
3. Failure-path test: malformed payload → assert no partial writes to either store, and (if implemented) DLQ topic receives the message.
4. Concurrency/partition test: publish messages across all 3 partitions concurrently → assert all are processed and persisted (validates `concurrency: 3` + virtual threads config).
5. `./gradlew test` (full suite, unit + integration) green in CI-like conditions; document any flakiness before sign-off.

---

## Status

- [x] Package rename decision confirmed: `com.kafkaplayground` → `com.library.consumer`
- [x] Gradle artifact-name corrections confirmed as part of Phase 1 setup
- [ ] Phase 1 code generation — **awaiting explicit go-ahead**
- [ ] Phase 2 code generation
- [ ] Phase 3 code generation
- [ ] Phase 4 code generation
