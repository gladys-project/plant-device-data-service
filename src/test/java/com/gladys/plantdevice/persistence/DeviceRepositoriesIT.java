package com.gladys.plantdevice.persistence;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.gladys.plantdevice.mapping.KafkaDeviceDataMapper;
import com.gladys.plantdevice.message.KafkaDeviceData;
import com.gladys.plantdevice.model.DeviceData;
import com.gladys.plantdevice.model.State;
import com.gladys.plantdevice.persistence.model.DeviceStateDocument;
import com.gladys.plantdevice.persistence.model.DiagnosticHistoryDocument;
import com.gladys.plantdevice.persistence.repository.DeviceStateRepository;
import com.gladys.plantdevice.persistence.repository.DiagnosticHistoryRepository;
import com.gladys.plantdevice.service.PlantDeviceDataService;
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
    @Autowired PlantDeviceDataService service;
    @Autowired KafkaDeviceDataMapper mapper;
    @Autowired JsonMapper json;

    private DeviceData observation(UUID deviceId) throws Exception {
        try (var input = new ClassPathResource("openapi/examples/kafka-device-data.json").getInputStream()) {
            var value = (ObjectNode) json.readTree(input);
            value.put("deviceId", deviceId.toString());
            return mapper.toDeviceData(json.treeToValue(value, KafkaDeviceData.class));
        }
    }

    @Test
    void stateRoundTripsDeviceUuidsAndKeepsDifferentDevicesSeparate() throws Exception {
        var id = UUID.randomUUID();
        var otherId = UUID.randomUUID();
        var seenAt = Instant.parse("2026-11-17T10:00:00Z");
        var state = new DeviceStateDocument(id, observation(id), seenAt, null);
        var otherState = new DeviceStateDocument(otherId, observation(otherId), seenAt.plusSeconds(10), null);

        var saved = Mono.when(states.save(state), states.save(otherState))
                .then(Mono.zip(states.findById(id), states.findById(otherId)));

        StepVerifier.create(saved).assertNext(result -> {
            assertThat(result.getT1().getDeviceId()).isEqualTo(id);
            assertThat(result.getT1().getLatestObservation()).isEqualTo(state.getLatestObservation());
            assertThat(result.getT1().getLastSeenAt()).isEqualTo(seenAt);
            assertThat(result.getT2().getDeviceId()).isEqualTo(otherId);
            assertThat(result.getT2().getLatestObservation()).isEqualTo(otherState.getLatestObservation());
            assertThat(result.getT2().getLastSeenAt()).isEqualTo(seenAt.plusSeconds(10));
        }).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void registersOnlyTheRequestedDeviceUuid() throws Exception {
        var id = UUID.randomUUID();
        var otherId = UUID.randomUUID();
        var initial = List.of(
                new DeviceStateDocument(id, observation(id), Instant.EPOCH, null),
                new DeviceStateDocument(otherId, observation(otherId), Instant.EPOCH, null));

        var registered = states.insert(initial).then(service.registerDevice(id, "Balcony Arduino"))
                .doOnNext(response -> {
                    assertThat(response.getDeviceId()).isEqualTo(id);
                    assertThat(response.getName()).isEqualTo("Balcony Arduino");
                    assertThat(response.getState().getValue()).isEqualTo("REGISTERED");
                }).then(Mono.zip(states.findById(id), states.findById(otherId)));

        StepVerifier.create(registered).assertNext(result -> {
            assertThat(result.getT1().getLatestObservation().getName()).isEqualTo("Balcony Arduino");
            assertThat(result.getT1().getLatestObservation().getState()).isEqualTo(State.REGISTERED);
            assertThat(result.getT2().getLatestObservation().getName()).isNull();
        }).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void incomingObservationsInsertThenUpdateTheRegisteredDevice() throws Exception {
        var id = UUID.randomUUID();
        var identity = id;
        var initial = observation(id);
        var incoming = observation(id);
        incoming.setEventId(UUID.randomUUID());
        incoming.setObservedAt(initial.getObservedAt() + 1000);
        incoming.setBatteryLevelPct(42);

        var updated = service.processDeviceData(initial)
                .then(service.registerDevice(id, "Balcony Arduino"))
                .then(states.findById(identity))
                .flatMap(before -> service.processDeviceData(incoming)
                        .then(states.findById(identity))
                        .doOnNext(after -> {
                            assertThat(after.getVersion()).isEqualTo(before.getVersion() + 1);
                            assertThat(after.getLastSeenAt()).isAfterOrEqualTo(before.getLastSeenAt());
                        }));

        StepVerifier.create(updated).assertNext(saved -> {
            assertThat(saved.getDeviceId()).isEqualTo(identity);
            assertThat(saved.getLatestObservation().getEventId()).isEqualTo(incoming.getEventId());
            assertThat(saved.getLatestObservation().getObservedAt()).isEqualTo(incoming.getObservedAt());
            assertThat(saved.getLatestObservation().getBatteryLevelPct()).isEqualTo(42);
            assertThat(saved.getLatestObservation().getName()).isEqualTo("Balcony Arduino");
            assertThat(saved.getLatestObservation().getState()).isEqualTo(State.REGISTERED);
        }).expectComplete().verify(Duration.ofSeconds(10));
    }

    @Test
    void aStaleStateSaveCannotOverwriteANewerVersion() throws Exception {
        var id = UUID.randomUUID();
        var identity = id;
        var seenAt = Instant.parse("2026-11-17T10:00:00Z");
        var updatedAt = seenAt.plusSeconds(10);
        var initial = new DeviceStateDocument(identity, observation(id), seenAt, null);
        var staleSave = states.save(initial).flatMap(current -> {
            var stale = new DeviceStateDocument(current.getDeviceId(), current.getLatestObservation(),
                    current.getLastSeenAt(), current.getVersion());
            current.setLastSeenAt(updatedAt);
            return states.save(current).flatMap(updated -> {
                assertThat(updated.getVersion()).isEqualTo(stale.getVersion() + 1);
                return states.save(stale);
            });
        });

        StepVerifier.create(staleSave).expectError(OptimisticLockingFailureException.class)
                .verify(Duration.ofSeconds(10));
        StepVerifier.create(states.findById(identity))
                .assertNext(saved -> assertThat(saved.getLastSeenAt()).isEqualTo(updatedAt))
                .expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    void historyQueriesAreScopedOrderedAndBoundedAndDuplicateInsertsFail() throws Exception {
        var id = UUID.randomUUID();
        var identity = id;
        var otherDevice = UUID.randomUUID();
        var data = observation(id);
        var time = Instant.parse("2026-11-17T10:00:00Z");
        var rows = List.of(
                change("older", identity, data, time.minusSeconds(10)),
                change("b", identity, data, time),
                change("a", identity, data, time),
                change("other", otherDevice, data, time.plusSeconds(10)));

        var page = history.insert(Flux.fromIterable(rows))
                .thenMany(history.findByDeviceIdOrderByRecordedAtDescIdAsc(identity, PageRequest.of(0, 2)));

        StepVerifier.create(page).expectNext(rows.get(2), rows.get(1))
                .expectComplete().verify(Duration.ofSeconds(10));
        StepVerifier.create(history.insert(rows.getFirst())).expectError(DuplicateKeyException.class)
                .verify(Duration.ofSeconds(5));
    }

    private DiagnosticHistoryDocument change(String id, UUID deviceId, DeviceData data, Instant time) {
        return new DiagnosticHistoryDocument(id, deviceId, data.getEventId(), data.getErrors().getFirst(),
                DiagnosticHistoryDocument.Change.OPENED, data.getObservedAt(), time);
    }
}
