# plant-device-data-service

Java 25 configuration skeleton for Gladys device data. The Maven artifact, application, image and Compose service are named `plant-device-data-service`; the checkout folder is currently `device-data-service`.

Includes WebFlux/Actuator, Spring Cloud Stream's Kafka binder, reactive MongoDB auto-configuration, generated inbound DTOs, Lombok/SLF4J, Maven Wrapper and infrastructure tests. The service can create a current-state document for the first observation of a device identity. The Kafka listener remains inactive.

## Inbound contract and bindings

[kafka-device-data.yml](src/main/resources/openapi/kafka-device-data.yml) is an OpenAPI 3.1.2 definition matching the device-data-processor's outbound `KafkaDeviceData` v1. It is self-contained, including `DeviceError`; no sibling checkout is required. The [example](src/main/resources/openapi/examples/kafka-device-data.json) preserves the original observation metadata, device ID, battery and errors. Sensor measurements remain on the upstream raw telemetry path and are absent from this message.

OpenAPI Generator creates `KafkaDeviceData` and `DeviceError` in `com.gladys.plantdevice.message` during `generate-sources`, under ignored `target/`. Jakarta constraints are generated from the schema; a future handler must invoke validation when processing. Models and validation metadata are compiled into the JAR. No APIs/controllers or outbound models are generated. `KafkaDeviceDataMapper` maps the generated input to a mutable internal `DeviceData` class, preserving every field and copying diagnostics into an immutable list of records. Strict unmapped-source/target checks catch contract drift during compilation. The generated schema-version enum is converted using its numeric value, preserving `1`. This model has no MongoDB entity annotation or persistence behavior; there is no active code invoking the mapper on broker records yet.

The inbound mapper leaves the optional `DeviceData.name` unset and initializes the observation to `State.UNNAMED`. The service uses that initial state when it creates the first document for a device identity. A reader can display **Unnamed device** when no name has been assigned.

| Declaration | Default |
| --- | --- |
| Future inbound binding | `onDeviceDataEvent-in-0` |
| Kafka topic | `gladys.device.data.v1` |
| Consumer group | `gladys.plant-device-data-service.v1` |
| Intended Kafka key | `deviceId` |

The configured function name is `onDeviceDataEvent`, matching the user-authored listener draft. Autodetection is off and consumer auto-startup is false in every profile; this change does not register or activate a processing function. No outbound binding is declared. There is no Kafka-to-Mongo processing flow yet. Topic provisioning is external; application and binder topic creation/partition additions are disabled.

## Persistence definitions

The wire DTO and internal `DeviceData` stay free of MongoDB annotations. Persistence models live under `persistence.model`, and Spring Data interfaces under `persistence.repository`. `DeviceStateDocument` is a mutable class with a fixed ID. A caller can edit a loaded document and save it using its existing version.

`DeviceStateDocumentMapper.toDocument(data, receivedAt, version)` builds a state document with a copy of the observation, its mock-aware identity and the supplied server receipt time. Pass `null` as the version for an insert or pass the version loaded from MongoDB for an update.

| Collection / repository | Stored data and operations |
| --- | --- |
| `device_states` / `DeviceStateRepository` | One latest observation per `DeviceIdentity(deviceId, mock)`, server-side `lastSeenAt`, and optimistic-lock `version`; standard reactive CRUD |
| `device_diagnostic_history` / `DiagnosticHistoryRepository` | Source event ID, diagnostic, `OPENED`/`UPDATED`/`RESOLVED`, original epoch `observedAt`, and server `recordedAt`; reactive CRUD/insert and bounded device-scoped history reads |

`DeviceIdentity` is serializable and serves as the compound state ID. It separates mock and real state for the same UUID. Callers must keep this identity consistent with the embedded observation. All UUIDs use Boot's `spring.mongodb.representation.uuid=standard` setting. `@Version` rejects stale saves with acknowledged MongoDB writes; it does not decide whether an incoming observation is chronologically newer. Callers retain the loaded version when updating a state document.

Diagnostic history documents are mutable classes with fixed literal string IDs (`@MongoId`). Supply a stable ID for each diagnostic transition and use `insert` for append semantics; inserting an existing ID fails. One source event can produce multiple diagnostic changes, so `sourceEventId` alone is not a unique transition ID. The repository does not generate those IDs, infer transitions or enforce append-only access on its inherited CRUD methods.

`findByDeviceOrderByRecordedAtDescIdAsc(identity, pageable)` filters by both device and mock flag, sorts newest server timestamp first with an ID tie-breaker, and accepts a bounded `PageRequest`. Index creation stays disabled at application startup. Provision the declared history index separately if needed:

```javascript
db.device_diagnostic_history.createIndex(
  {device: 1, recordedAt: -1, _id: 1},
  {name: "device_history"}
)
```

No retention/TTL policy or notification outbox is added. The service owns future decisions about freshness, ordering, active error snapshots, battery thresholds and notification consistency; these are not repository responsibilities.

## Initial device state

`PlantDeviceDataService` exposes this observation processing method:

```java
Mono<Void> processDeviceData(DeviceData data)
```

The service looks up the identity using both `deviceId` and `mock`. For a new identity, it saves the first observation with the server time and lets Spring Data initialize the version. For a known identity, it completes without writing. The returned publisher reports completion or a database error. State updates, diagnostic reconciliation and notifications are future steps.

`registerDevice(deviceId, name)` sets or changes the name for every stored entry matching the device UUID and sets its registration state to `REGISTERED`. It trims the name and reports an unknown UUID as an error. The API layer is responsible for request validation. Mock and real observations keep their own stored entries.

`diagnosticHistoryDocument(deviceId, size)` reads the requested number of most recent diagnostic entries for the real device.

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

Compose selects `deployed`, enabling MongoDB health while keeping consumption inactive. The Dockerfile packages only the verified JAR on a pinned Java 25 JRE and runs as UID/GID `10001`. Environment files and source are excluded from the image build context.

## CI, deployment and releases

GitHub Actions uses the organization’s Linux self-hosted runner with Docker access. CI runs on branch pushes, same-repository pull requests and manual dispatch. It sets up Java 25 and Compose v5.6.0, runs `./mvnw clean verify` with real Kafka and MongoDB test containers, validates the service Compose file and builds the image. Test reports and the verified JAR are uploaded separately.

Deployment runs after a pull request merges into `main`, or manually for a selected branch. Both paths reuse CI and deploy the verified commit’s JAR. Deployments share one queue and update only the `plant-device-data-service` Compose service with a commit-tagged image. Once the workflow is on `main`, select a branch from **Actions → Deploy plant device data service → Run workflow**, or run:

```bash
gh workflow run deploy.yml --ref feat/plant-device-data-service-foundation
```

The deploy job targets the server’s Docker daemon. Set `GLADYS_ENV_FILE` as a GitHub repository or production environment variable only to override `/etc/gladys/gladys.env`. If the runner cannot read that file directly, the script reads it through a read-only host bind into a private temporary file and removes the copy on exit. The external network and services must already exist. Deploy waits for the container to start. Check MongoDB readiness separately at `http://127.0.0.1:8082/actuator/health`.

Pushing a `v*` tag publishes the verified JAR to GitHub Releases. The publication runner needs `gh` on PATH. The release workflow is separate from deployment. All action references are pinned by commit SHA.

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
