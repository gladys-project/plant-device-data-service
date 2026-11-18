# plant-device-data-service

Java 25 configuration skeleton for Gladys device data. The Maven artifact, application, image and Compose service are named `plant-device-data-service`; the checkout folder is currently `device-data-service`.

Includes WebFlux/Actuator, Spring Cloud Stream's Kafka binder, reactive MongoDB auto-configuration, generated inbound DTOs, Lombok/SLF4J, Maven Wrapper and infrastructure tests. Includes the standard Boot entry point, generated models, structural MapStruct mapper, MongoDB document records/reactive repositories and service method definitions. The existing listener draft is preserved. No device reconciliation, battery rules, notification publishing or application persistence flow is implemented.

## Inbound contract and bindings

[kafka-device-data.yml](src/main/resources/openapi/kafka-device-data.yml) is an OpenAPI 3.1.2 definition matching the device-data-processor's outbound `KafkaDeviceData` v1. It is self-contained, including `DeviceError`; no sibling checkout is required. The [example](src/main/resources/openapi/examples/kafka-device-data.json) preserves the original observation metadata, device ID, battery and errors. Sensor measurements remain on the upstream raw telemetry path and are absent from this message.

OpenAPI Generator creates `KafkaDeviceData` and `DeviceError` in `com.gladys.plantdevice.message` during `generate-sources`, under ignored `target/`. Jakarta constraints are generated from the schema; a future handler must invoke validation when processing. Models and validation metadata are compiled into the JAR. No APIs/controllers or outbound models are generated. `KafkaDeviceDataMapper` maps the generated input to an immutable internal `DeviceData` record, preserving every field and copying diagnostics into an immutable list of records. Strict unmapped-source/target checks catch contract drift during compilation. The generated schema-version enum is converted using its numeric value, preserving `1`. This model has no MongoDB entity annotation or persistence behavior; there is no active code invoking the mapper on broker records yet.

| Declaration | Default |
| --- | --- |
| Future inbound binding | `onDeviceDataEvent-in-0` |
| Kafka topic | `gladys.device.data.v1` |
| Consumer group | `gladys.plant-device-data-service.v1` |
| Intended Kafka key | `deviceId` |

The configured function name is `onDeviceDataEvent`, matching the user-authored listener draft. Autodetection is off and consumer auto-startup is false in every profile; this change does not register or activate a processing function. No outbound binding is declared. There is no Kafka-to-Mongo processing flow yet. Topic provisioning is external; application and binder topic creation/partition additions are disabled.

## Persistence definitions

The wire DTO and internal `DeviceData` stay free of MongoDB annotations. Persistence records live under `persistence.model`, and Spring Data interfaces under `persistence.repository`.

| Collection / repository | Stored data and operations |
| --- | --- |
| `device_states` / `DeviceStateRepository` | One latest observation per `DeviceIdentity(deviceId, mock)`, server-side `lastSeenAt`, and optimistic-lock `version`; standard reactive CRUD |
| `device_diagnostic_history` / `DiagnosticHistoryRepository` | Source event ID, diagnostic, `OPENED`/`UPDATED`/`RESOLVED`, original epoch `observedAt`, and server `recordedAt`; reactive CRUD/insert and bounded device-scoped history reads |

`DeviceIdentity` is serializable and serves as the compound state ID. It separates mock and real state for the same UUID. Callers must keep this identity consistent with the embedded observation. All UUIDs use Boot's `spring.mongodb.representation.uuid=standard` setting. `@Version` rejects stale saves with acknowledged MongoDB writes; it does not decide whether an incoming observation is chronologically newer. Callers retain the loaded version when updating a state document.

History uses literal string IDs (`@MongoId`). Supply a stable ID for each diagnostic transition and use `insert` for append semantics; inserting an existing ID fails. One source event can produce multiple diagnostic changes, so `sourceEventId` alone is not a unique transition ID. The repository does not generate those IDs, infer transitions or enforce append-only access on its inherited CRUD methods.

`findByDeviceOrderByRecordedAtDescIdAsc(identity, pageable)` filters by both device and mock flag, sorts newest server timestamp first with an ID tie-breaker, and accepts a bounded `PageRequest`. Index creation stays disabled at application startup. Provision the declared history index separately if needed:

```javascript
db.device_diagnostic_history.createIndex(
  {device: 1, recordedAt: -1, _id: 1},
  {name: "device_history"}
)
```

No retention/TTL policy or notification outbox is added. The service owns future decisions about freshness, ordering, active error snapshots, battery thresholds and notification consistency; these are not repository responsibilities.

## Service method definitions

`PlantDeviceDataService` exposes exactly one public processing method:

```java
Mono<Void> processDeviceData(DeviceData data)
```

Its private stages declare state loading, state reconciliation, diagnostic comparison, battery evaluation, persistence orchestration and notification requests. All stages are explicitly unimplemented. The entrypoint returns an error publisher instead of a successful no-op and performs no repository operations. The existing listener is not wired to this entrypoint by this change. No thresholds, error-clearing assumptions, database-write orchestration or Kafka publishing are implemented.

## Configuration and startup

Use JDK 25 and the checked-in Maven Wrapper. `application.yml` holds shared defaults, `application-local.yml` host settings and `application-test.yml` isolated test overrides importing the defaults. Standard auto-configuration supplies `KafkaTemplate`, factories, reactive `MongoClient` and `ReactiveMongoTemplate`; no custom infrastructure layers are needed. The database is selected once in the MongoDB URI. Mongo index creation is disabled.

| Process environment | Default / purpose |
| --- | --- |
| `SERVER_PORT` | `8082`; Compose uses container port `8080` |
| `SPRING_KAFKA_BOOTSTRAPSERVERS` | `localhost:9092`; Compose defaults to `kafka:29092` |
| `SPRING_MONGODB_URI` | `mongodb://localhost:27017/plant_device_data`; include credentials, database and TLS options where required |
| `DEVICE_DATA_TOPIC` | `gladys.device.data.v1` |
| `PLANT_DEVICE_DATA_GROUP` | `gladys.plant-device-data-service.v1` |
| `SPRING_PROFILES_ACTIVE` | Unset by default; `local` for host settings, `deployed` enables MongoDB health |
| `PLANT_DEVICE_DATA_HTTP_PORT` | Compose host port `8082` |
| `GLADYS_NETWORK` | Existing external Compose network `gladys` |
| `PLANT_DEVICE_DATA_IMAGE` | Compose image override; default `plant-device-data-service:local` |

Kafka authentication uses standard `SPRING_KAFKA_SECURITY_PROTOCOL`, `SPRING_KAFKA_PROPERTIES_SASL_MECHANISM` and `SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG` settings. Spring Boot 4's URI property is **`spring.mongodb.uri`**, not the old `spring.data.mongodb.uri`.

Start the HTTP skeleton from the host:

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
curl --fail http://localhost:8082/actuator/health
```

The driver may attempt background MongoDB monitoring, but startup and default health do not require reachable infrastructure. Only health/info are exposed. Add `deployed` to the profile list to include the real MongoDB health check; this may return `DOWN` when MongoDB is unavailable. Health never claims that a Kafka consumer is running.

For a host process, export actual settings explicitly; Spring does not load `.env` automatically:

```bash
export SPRING_KAFKA_BOOTSTRAPSERVERS=localhost:9092
export SPRING_MONGODB_URI=mongodb://localhost:27017/plant_device_data
./mvnw spring-boot:run -Dspring-boot.run.profiles=local,deployed
```

For server deployment, merge the required values into the existing **`/etc/gladys/gladys.env`** (mode `600`), using `.env.example` as a placeholder reference. Use container-reachable broker addresses and the actual MongoDB database/credentials. Keep the shared file outside Git. `.gitignore` excludes local env/secret files, keystores, build outputs, IDE state, logs, caches and runtime data; the Maven Wrapper, OpenAPI source and placeholder `.env.example` remain versioned. Compose forwards only this service's declared settings, rather than the whole shared env file.

Root Compose contains only this application. Kafka, MongoDB and the external `gladys` network must already exist and are managed independently. Build the verified JAR, then start the service:

```bash
./mvnw clean verify
docker compose --env-file /etc/gladys/gladys.env config --quiet
docker compose --env-file /etc/gladys/gladys.env up -d --build
```

Compose selects `deployed`, enabling MongoDB health while keeping consumption inactive. The Dockerfile packages only the verified JAR on a pinned Java 25 JRE and runs as UID/GID `10001`. Environment files and source are excluded from the image build context. No CI/CD workflows or server changes are included in this iteration.

## Checks and TDD

```bash
./mvnw test          # Fast checks; no Docker or reachable infrastructure required
./mvnw clean verify  # Also Kafka + MongoDB integration tests; Docker required
```

Surefire runs `*Test`; Failsafe runs `*IT` during `integration-test` and `verify`. Integration tests use pinned `apache/kafka:4.1.0` and `mongo:8.0.17`, explicit readiness and mapped addresses. They verify configured Kafka key/payload delivery and real reactive MongoDB write/read/delete using test-only data. Repository integration checks additionally verify UUID/document round trips, mock/real separation, optimistic locking, deterministic history pagination and duplicate insert rejection. Kafka topic provisioning is enabled only in the integration fixture. Docker unavailability fails verification; tests never silently skip. StepVerifier tests actual HTTP/Mongo publishers, and all waits have limits.

TDD increments: bootstrap test failed without a Boot configuration; configuration tests failed before defaults/bindings were added; generated-model tests failed before OpenAPI generation was configured; mapper tests failed before the mapper existed; a client-address assertion exposed test import precedence and passed after test overrides were placed in a later YAML document. Repository/model tests also failed before their definitions existed, real MongoDB checks exposed UUID representation and serializable-ID requirements, and service contract tests failed before its entrypoint existed. Each increment was run again after implementation. Test fixtures were corrected where framework property names required it; these corrections are not claimed as production regressions. Integration checks verify infrastructure only, not a business message-processing pipeline.

## Versions and compatibility

Java **25**, Spring Boot **4.1.2**, Spring Cloud BOM **2025.1.4**, Maven **3.9.12** (Wrapper **3.3.4**, distribution SHA-256 verified), OpenAPI Generator **7.26.0**, MapStruct **1.6.3**, lombok-mapstruct-binding **0.2.0**. Boot/Cloud manage Kafka, Reactor, MongoDB drivers, Spring Data, Lombok, SLF4J, JUnit and Testcontainers versions; do not pin them separately. Compiler **3.15.0** and Surefire/Failsafe **3.5.6** come from the pinned Boot parent. Lombok and MapStruct annotation processing are explicitly configured for Java 25. No preview features are enabled. Managed versions include Kafka clients **4.2.2**, Spring Kafka **4.1.2**, MongoDB Reactive Streams driver **5.8.1**, Reactor **3.8.8**, Lombok **1.18.48**, SLF4J **2.0.20**, JUnit Jupiter **6.0.3** and Testcontainers **2.0.5**.

Official references: [Boot Java/Maven compatibility](https://docs.spring.io/spring-boot/system-requirements.html), [Cloud/Boot compatibility](https://spring.io/projects/spring-cloud/), [reactive MongoDB auto-configuration](https://docs.spring.io/spring-boot/reference/data/nosql.html), [Cloud binding properties](https://docs.spring.io/spring-cloud-stream/reference/spring-cloud-stream/binding-properties.html), [OpenAPI Spring generator](https://openapi-generator.tech/docs/generators/spring/), [Testcontainers MongoDB](https://java.testcontainers.org/modules/databases/mongodb/), [MongoDB repository queries](https://docs.spring.io/spring-data/mongodb/reference/mongodb/repositories/query-methods.html), [MongoDB optimistic locking](https://docs.spring.io/spring-data/mongodb/reference/mongodb/template-crud-operations.html#mongo-template.optimistic-locking), [MapStruct and Lombok integration](https://mapstruct.org/documentation/stable/reference/html/#lombok).

Follow [AGENT.md](AGENT.md) for project guidance; [AGENTS.md](AGENTS.md) points to that authoritative source. Keep future request/event paths reactive, reuse standard configuration and generated constraints, and add meaningful tests before core logic. Regular Spring Kafka/Cloud bindings do not provide end-to-end Reactor backpressure.

Upstream sensor payloads now use UUID `sensorId` and typed `rawMoisture`, `rawMoistureMin`, `rawMoistureMax`, `sampleCount` and optional `calibrationId`. Those fields travel on raw telemetry and are intentionally excluded by the device processor from `KafkaDeviceData`. This service’s inbound OpenAPI and MapStruct model therefore remain device-only and match the unchanged device-data output; do not add sensor fields to device state/history just to mirror raw telemetry.
