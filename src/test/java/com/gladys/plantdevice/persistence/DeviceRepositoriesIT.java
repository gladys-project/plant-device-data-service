package com.gladys.plantdevice.persistence;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.gladys.plantdevice.mapping.KafkaDeviceDataMapper;
import com.gladys.plantdevice.message.KafkaDeviceData;
import com.gladys.plantdevice.model.DeviceData;
import com.gladys.plantdevice.persistence.model.DeviceIdentity;
import com.gladys.plantdevice.persistence.model.DeviceStateDocument;
import com.gladys.plantdevice.persistence.model.DiagnosticHistoryDocument;
import com.gladys.plantdevice.persistence.repository.DeviceStateRepository;
import com.gladys.plantdevice.persistence.repository.DiagnosticHistoryRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "spring.config.location=classpath:/application-test.yml")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Timeout(30)
class DeviceRepositoriesIT {
    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8.0.17")
            .withReplicaSet()
            .waitingFor(Wait.forLogMessage(".*Waiting for connections.*\\n", 1))
            .withStartupTimeout(Duration.ofSeconds(90));

    @DynamicPropertySource
    static void mongodb(DynamicPropertyRegistry properties) {
        properties.add("spring.mongodb.uri", () -> MONGO.getReplicaSetUrl("device_repositories_it"));
    }

    @Autowired DeviceStateRepository states;
    @Autowired DiagnosticHistoryRepository history;
    @Autowired KafkaDeviceDataMapper mapper;
    @Autowired JsonMapper json;

    private DeviceData observation(UUID deviceId, boolean mock) throws Exception {
        try (var input = new ClassPathResource("openapi/examples/kafka-device-data.json").getInputStream()) {
            var value = (ObjectNode) json.readTree(input);
            value.put("deviceId", deviceId.toString()).put("mock", mock);
            return mapper.toDeviceData(json.treeToValue(value, KafkaDeviceData.class));
        }
    }

    @Test
    void stateRoundTripsUuidsAndStoresMockAndRealObservationsSeparately() throws Exception {
        var id = UUID.randomUUID();
        var real = new DeviceIdentity(id, false);
        var mock = new DeviceIdentity(id, true);
        var seenAt = Instant.parse("2026-11-17T10:00:00Z");
        var realState = new DeviceStateDocument(real, observation(id, false), seenAt, null);
        var mockState = new DeviceStateDocument(mock, observation(id, true), seenAt.plusSeconds(10), null);

        var saved = Mono.when(states.save(realState), states.save(mockState))
                .then(Mono.zip(states.findById(real), states.findById(mock)));

        StepVerifier.create(saved).assertNext(result -> {
            assertThat(result.getT1().id()).isEqualTo(real);
            assertThat(result.getT1().latestObservation()).isEqualTo(realState.latestObservation());
            assertThat(result.getT1().lastSeenAt()).isEqualTo(seenAt);
            assertThat(result.getT2().id()).isEqualTo(mock);
            assertThat(result.getT2().latestObservation()).isEqualTo(mockState.latestObservation());
            assertThat(result.getT2().lastSeenAt()).isEqualTo(seenAt.plusSeconds(10));
        }).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void aStaleStateSaveCannotOverwriteANewerVersion() throws Exception {
        var id = UUID.randomUUID();
        var identity = new DeviceIdentity(id, false);
        var seenAt = Instant.parse("2026-11-17T10:00:00Z");
        var initial = new DeviceStateDocument(identity, observation(id, false), seenAt, null);
        var staleSave = states.save(initial).flatMap(original ->
                states.save(new DeviceStateDocument(identity, original.latestObservation(),
                                seenAt.plusSeconds(10), original.version()))
                        .flatMap(updated -> {
                            assertThat(updated.version()).isEqualTo(original.version() + 1);
                            return states.save(original);
                        }));

        StepVerifier.create(staleSave).expectError(OptimisticLockingFailureException.class)
                .verify(Duration.ofSeconds(10));
        StepVerifier.create(states.findById(identity))
                .assertNext(saved -> assertThat(saved.lastSeenAt()).isEqualTo(seenAt.plusSeconds(10)))
                .expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    void historyQueriesAreScopedOrderedAndBoundedAndDuplicateInsertsFail() throws Exception {
        var id = UUID.randomUUID();
        var identity = new DeviceIdentity(id, false);
        var otherDevice = new DeviceIdentity(UUID.randomUUID(), false);
        var mockDevice = new DeviceIdentity(id, true);
        var data = observation(id, false);
        var time = Instant.parse("2026-11-17T10:00:00Z");
        var rows = List.of(
                change("older", identity, data, time.minusSeconds(10)),
                change("b", identity, data, time),
                change("a", identity, data, time),
                change("mock", mockDevice, data, time.plusSeconds(10)),
                change("other", otherDevice, data, time.plusSeconds(10)));

        var page = history.insert(Flux.fromIterable(rows))
                .thenMany(history.findByDeviceOrderByRecordedAtDescIdAsc(identity, PageRequest.of(0, 2)));

        StepVerifier.create(page).expectNext(rows.get(2), rows.get(1))
                .expectComplete().verify(Duration.ofSeconds(10));
        StepVerifier.create(history.insert(rows.getFirst())).expectError(DuplicateKeyException.class)
                .verify(Duration.ofSeconds(5));
    }

    private DiagnosticHistoryDocument change(String id, DeviceIdentity device, DeviceData data, Instant time) {
        return new DiagnosticHistoryDocument(id, device, data.eventId(), data.errors().getFirst(),
                DiagnosticHistoryDocument.Change.OPENED, data.observedAt(), time);
    }
}
