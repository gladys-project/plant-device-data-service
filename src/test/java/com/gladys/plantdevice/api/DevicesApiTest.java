package com.gladys.plantdevice.api;

import java.time.Instant;
import java.util.UUID;
import java.util.Map;
import java.util.List;

import com.gladys.plantdevice.mapping.DeviceRegistrationMapperImpl;
import com.gladys.plantdevice.mapping.DeviceStateDocumentMapperImpl;
import com.gladys.plantdevice.mapping.DeviceSummaryMapperImpl;
import com.gladys.plantdevice.model.DeviceData;
import com.gladys.plantdevice.model.State;
import com.gladys.plantdevice.persistence.model.DeviceIdentity;
import com.gladys.plantdevice.persistence.model.DeviceStateDocument;
import com.gladys.plantdevice.persistence.repository.DeviceStateRepository;
import com.gladys.plantdevice.persistence.repository.DiagnosticHistoryRepository;
import com.gladys.plantdevice.service.PlantDeviceDataService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@WebFluxTest(DevicesApiController.class)
@Import({DeviceApiDelegateImpl.class, PlantDeviceDataService.class,
        DeviceStateDocumentMapperImpl.class, DeviceSummaryMapperImpl.class, DeviceRegistrationMapperImpl.class})
class DevicesApiTest {
    @Autowired WebTestClient web;
    @MockitoBean DeviceStateRepository states;
    @MockitoBean DiagnosticHistoryRepository history;

    @Test
    void listsNamedAndUnnamedEntriesForTheSameDevice() {
        var id = UUID.randomUUID();
        var seenAt = Instant.parse("2026-11-21T10:15:30Z");
        var registered = new DeviceData();
        registered.setName("Balcony Arduino");
        registered.setState(State.REGISTERED);
        var real = new DeviceStateDocument(new DeviceIdentity(id, false), registered, seenAt, 2L);
        var mock = new DeviceStateDocument(new DeviceIdentity(id, true), new DeviceData(), seenAt, 0L);
        when(states.findAll()).thenReturn(Flux.just(real, mock));

        web.get().uri("/devices").accept(MediaType.APPLICATION_JSON).exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.length()").isEqualTo(2)
                .jsonPath("$[0].deviceId").isEqualTo(id.toString())
                .jsonPath("$[0].mock").isEqualTo(false)
                .jsonPath("$[0].name").isEqualTo("Balcony Arduino")
                .jsonPath("$[0].state").isEqualTo("REGISTERED")
                .jsonPath("$[0].lastSeenAt").isEqualTo(seenAt.toString())
                .jsonPath("$[1].deviceId").isEqualTo(id.toString())
                .jsonPath("$[1].mock").isEqualTo(true)
                .jsonPath("$[1].name").doesNotExist()
                .jsonPath("$[1].state").isEqualTo("UNNAMED");
    }

    @Test
    void listsAllStoredObservationFieldsAndTheDocumentVersion() {
        var id = UUID.randomUUID();
        var eventId = UUID.randomUUID();
        var bootId = UUID.randomUUID();
        var observation = new DeviceData();
        observation.setSchemaVersion(1);
        observation.setDeviceId(id);
        observation.setEventId(eventId);
        observation.setEventType("SOIL_MOISTURE_READING");
        observation.setBootId(bootId);
        observation.setSequence(23L);
        observation.setUptimeMs(28500L);
        observation.setObservedAt(1794164428500L);
        observation.setClockSource("MANUAL_TEST");
        observation.setMock(false);
        observation.setName("Balcony Arduino");
        observation.setState(State.REGISTERED);
        observation.setBatteryLevelPct(10);
        observation.setSupplyVoltageMv(4750);
        observation.setErrors(List.of(new DeviceData.Diagnostic(
                "LOW_BATTERY", "BATTERY", "ERROR", "Battery below threshold.")));
        var seenAt = Instant.parse("2026-11-21T10:15:30Z");
        var document = new DeviceStateDocument(new DeviceIdentity(id, false), observation, seenAt, 2L);
        when(states.findAll()).thenReturn(Flux.just(document));

        web.get().uri("/devices").accept(MediaType.APPLICATION_JSON).exchange()
                .expectStatus().isOk().expectBody()
                .jsonPath("$[0].deviceId").isEqualTo(id.toString())
                .jsonPath("$[0].mock").isEqualTo(false)
                .jsonPath("$[0].name").isEqualTo("Balcony Arduino")
                .jsonPath("$[0].state").isEqualTo("REGISTERED")
                .jsonPath("$[0].lastSeenAt").isEqualTo(seenAt.toString())
                .jsonPath("$[0].version").isEqualTo(2)
                .jsonPath("$[0].schemaVersion").isEqualTo(1)
                .jsonPath("$[0].eventId").isEqualTo(eventId.toString())
                .jsonPath("$[0].eventType").isEqualTo("SOIL_MOISTURE_READING")
                .jsonPath("$[0].bootId").isEqualTo(bootId.toString())
                .jsonPath("$[0].sequence").isEqualTo(23)
                .jsonPath("$[0].uptimeMs").isEqualTo(28500)
                .jsonPath("$[0].observedAt").isEqualTo(1794164428500L)
                .jsonPath("$[0].clockSource").isEqualTo("MANUAL_TEST")
                .jsonPath("$[0].batteryLevelPct").isEqualTo(10)
                .jsonPath("$[0].supplyVoltageMv").isEqualTo(4750)
                .jsonPath("$[0].errors.length()").isEqualTo(1)
                .jsonPath("$[0].errors[0].code").isEqualTo("LOW_BATTERY")
                .jsonPath("$[0].errors[0].component").isEqualTo("BATTERY")
                .jsonPath("$[0].errors[0].severity").isEqualTo("ERROR")
                .jsonPath("$[0].errors[0].message").isEqualTo("Battery below threshold.");
    }

    @Test
    void listsHistoricalDocumentsWithMissingObservationMetadata() {
        var id = UUID.randomUUID();
        var seenAt = Instant.parse("2026-11-21T10:15:30Z");
        var document = new DeviceStateDocument(new DeviceIdentity(id, false), null, seenAt, null);
        when(states.findAll()).thenReturn(Flux.just(document));

        web.get().uri("/devices").accept(MediaType.APPLICATION_JSON).exchange()
                .expectStatus().isOk().expectBody()
                .jsonPath("$[0].deviceId").isEqualTo(id.toString())
                .jsonPath("$[0].mock").isEqualTo(false)
                .jsonPath("$[0].state").isEqualTo("UNNAMED")
                .jsonPath("$[0].lastSeenAt").isEqualTo(seenAt.toString())
                .jsonPath("$[0].eventId").doesNotExist()
                .jsonPath("$[0].batteryLevelPct").doesNotExist()
                .jsonPath("$[0].supplyVoltageMv").doesNotExist();
    }

    @Test
    void returnsAnEmptyArrayWhenThereAreNoSavedDevices() {
        when(states.findAll()).thenReturn(Flux.empty());

        web.get().uri("/devices").accept(MediaType.APPLICATION_JSON).exchange()
                .expectStatus().isOk()
                .expectBody().json("[]");
    }

    @Test
    void registersRealAndMockEntriesAndReturnsTheSavedNameAndState() {
        var id = UUID.randomUUID();
        var real = new DeviceStateDocument(new DeviceIdentity(id, false), new DeviceData(), Instant.EPOCH, 0L);
        var mock = new DeviceStateDocument(new DeviceIdentity(id, true), new DeviceData(), Instant.EPOCH, 0L);
        when(states.findAllById_DeviceId(id)).thenReturn(Flux.just(real, mock));
        when(states.save(any(DeviceStateDocument.class))).thenAnswer(invocation ->
                Mono.just(invocation.getArgument(0, DeviceStateDocument.class)));

        web.post().uri("/register").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("deviceId", id.toString(), "name", "  Balcony Arduino  "))
                .exchange().expectStatus().isOk().expectBody()
                .jsonPath("$.deviceId").isEqualTo(id.toString())
                .jsonPath("$.name").isEqualTo("Balcony Arduino")
                .jsonPath("$.state").isEqualTo("REGISTERED");

        verify(states).save(real);
        verify(states).save(mock);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"deviceId\":\"invalid\",\"name\":\"Balcony\"}",
            "{\"deviceId\":\"550e8400-e29b-41d4-a716-446655440000\",\"name\":\" \"}"})
    void rejectsInvalidRegistrationRequestsBeforeCallingTheRepository(String body) {
        web.post().uri("/register").contentType(MediaType.APPLICATION_JSON).bodyValue(body)
                .exchange().expectStatus().isBadRequest();

        verifyNoInteractions(states);
    }

    @Test
    void requiresARegistrationRequestBody() {
        web.post().uri("/register").contentType(MediaType.APPLICATION_JSON)
                .exchange().expectStatus().isBadRequest();

        verifyNoInteractions(states);
    }

    @Test
    void reportsAnUnknownDeviceAsNotFound() {
        var id = UUID.randomUUID();
        when(states.findAllById_DeviceId(id)).thenReturn(Flux.empty());

        web.post().uri("/register").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("deviceId", id.toString(), "name", "Balcony Arduino"))
                .exchange().expectStatus().isNotFound();
    }

    @Test
    void reportsAPersistenceFailureWithoutReturningASuccessBody() {
        var id = UUID.randomUUID();
        var state = new DeviceStateDocument(new DeviceIdentity(id, false), new DeviceData(), Instant.EPOCH, 0L);
        when(states.findAllById_DeviceId(id)).thenReturn(Flux.just(state));
        when(states.save(state)).thenReturn(Mono.error(new IllegalStateException("MongoDB write failed")));

        web.post().uri("/register").contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("deviceId", id.toString(), "name", "Balcony Arduino"))
                .exchange().expectStatus().is5xxServerError();
    }
}
