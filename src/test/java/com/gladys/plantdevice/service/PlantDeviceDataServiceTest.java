package com.gladys.plantdevice.service;

import java.lang.reflect.Modifier;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import com.gladys.plantdevice.mapping.DeviceStateDocumentMapper;
import com.gladys.plantdevice.mapping.DeviceSummaryMapper;
import com.gladys.plantdevice.model.DeviceData;
import com.gladys.plantdevice.model.State;
import com.gladys.plantdevice.persistence.model.DeviceIdentity;
import com.gladys.plantdevice.persistence.model.DeviceStateDocument;
import com.gladys.plantdevice.persistence.repository.DeviceStateRepository;
import com.gladys.plantdevice.persistence.repository.DiagnosticHistoryRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mapstruct.factory.Mappers;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PlantDeviceDataServiceTest {
    private final DeviceStateRepository states = mock(DeviceStateRepository.class);
    private final DeviceStateDocumentMapper mapper = Mappers.getMapper(DeviceStateDocumentMapper.class);
    private final PlantDeviceDataService service =
            new PlantDeviceDataService(states, mock(DiagnosticHistoryRepository.class), mapper,
                    Mappers.getMapper(DeviceSummaryMapper.class));

    @Test
    void exposesProcessingRegistrationHistoryAndListingEntrypoints() throws Exception {
        var methods = Arrays.stream(PlantDeviceDataService.class.getDeclaredMethods())
                .filter(method -> !method.isSynthetic()).toList();
        assertThat(methods.stream().filter(method -> Modifier.isPublic(method.getModifiers()))
                .map(method -> method.getName()))
                .containsExactlyInAnyOrder("processDeviceData", "registerDevice", "diagnosticHistoryDocument", "listDevices");
        assertThat(methods.stream().filter(method -> !Modifier.isPublic(method.getModifiers())))
                .allSatisfy(method -> assertThat(Modifier.isPrivate(method.getModifiers())).isTrue());
        assertThat(PlantDeviceDataService.class.getMethod("processDeviceData", DeviceData.class).getReturnType())
                .isEqualTo(reactor.core.publisher.Mono.class);
    }

    @Test
    void registersAndRenamesAllEntriesForTheSameDeviceId() {
        var observation = observation();
        var mockState = mapper.toDocument(observation, Instant.EPOCH, 0L);
        observation.setMock(false);
        observation.setName("Old name");
        observation.setState(State.REGISTERED);
        var realState = mapper.toDocument(observation, Instant.EPOCH, 2L);
        when(states.findAllById_DeviceId(observation.getDeviceId())).thenReturn(Flux.just(mockState, realState));
        when(states.save(any(DeviceStateDocument.class))).thenAnswer(invocation ->
                Mono.just(invocation.getArgument(0, DeviceStateDocument.class)));

        StepVerifier.create(service.registerDevice(observation.getDeviceId(), "  Balcony Arduino  "))
                .expectComplete().verify(Duration.ofSeconds(1));

        for (var state : List.of(mockState, realState)) {
            verify(states).save(state);
            assertThat(state.getLatestObservation().getName()).isEqualTo("Balcony Arduino");
            assertThat(state.getLatestObservation().getState()).isEqualTo(State.REGISTERED);
        }
    }

    @Test
    void reportsAnUnknownDeviceIdWithoutSaving() {
        var deviceId = UUID.randomUUID();
        when(states.findAllById_DeviceId(deviceId)).thenReturn(Flux.empty());

        StepVerifier.create(service.registerDevice(deviceId, "Balcony Arduino"))
                .expectError(NoSuchElementException.class).verify(Duration.ofSeconds(1));

        verify(states, never()).save(any(DeviceStateDocument.class));
    }

    @Test
    void propagatesRegistrationSaveFailure() {
        var observation = observation();
        var state = mapper.toDocument(observation, Instant.EPOCH, 0L);
        var failure = new IllegalStateException("MongoDB write failed");
        when(states.findAllById_DeviceId(observation.getDeviceId())).thenReturn(Flux.just(state));
        when(states.save(state)).thenReturn(Mono.error(failure));

        StepVerifier.create(service.registerDevice(observation.getDeviceId(), "Balcony Arduino"))
                .expectErrorMatches(error -> error == failure).verify(Duration.ofSeconds(1));
    }

    @Test
    void savesFirstObservationWithANewDocumentVersionOnSubscription() {
        var observation = observation();
        var identity = new DeviceIdentity(observation.getDeviceId(), true);
        when(states.findById(identity)).thenReturn(Mono.empty());
        when(states.save(any(DeviceStateDocument.class))).thenAnswer(invocation ->
                Mono.just(invocation.getArgument(0, DeviceStateDocument.class)));

        var result = service.processDeviceData(observation);

        verify(states, never()).save(any(DeviceStateDocument.class));
        StepVerifier.create(result).expectComplete().verify(Duration.ofSeconds(1));

        var document = ArgumentCaptor.forClass(DeviceStateDocument.class);
        verify(states).save(document.capture());
        assertThat(document.getValue().getId()).isEqualTo(identity);
        assertThat(document.getValue().getLatestObservation()).isEqualTo(observation);
        assertThat(document.getValue().getLastSeenAt()).isNotNull();
        assertThat(document.getValue().getVersion()).isNull();
    }

    @ParameterizedTest
    @ValueSource(longs = {1000, 2000})
    void updatesAnExistingDeviceAndPreservesItsRegistration(long observedAt) {
        var observation = observation();
        observation.setObservedAt(1000);
        observation.setName("Balcony Arduino");
        observation.setState(State.REGISTERED);
        var existing = mapper.toDocument(observation, Instant.EPOCH, 4L);
        var incoming = mapper.copyObservation(observation);
        incoming.setObservedAt(observedAt);
        incoming.setEventId(UUID.randomUUID());
        incoming.setBatteryLevelPct(42);
        incoming.setName(null);
        incoming.setState(State.UNNAMED);
        when(states.findById(existing.getId())).thenReturn(Mono.just(existing));
        when(states.save(existing)).thenReturn(Mono.just(existing));
        var receivedAfter = Instant.now();

        var result = service.processDeviceData(incoming);
        verify(states, never()).save(any(DeviceStateDocument.class));
        StepVerifier.create(result)
                .expectComplete().verify(Duration.ofSeconds(1));

        verify(states).save(existing);
        assertThat(existing.getLatestObservation().getEventId()).isEqualTo(incoming.getEventId());
        assertThat(existing.getLatestObservation().getObservedAt()).isEqualTo(observedAt);
        assertThat(existing.getLatestObservation().getBatteryLevelPct()).isEqualTo(42);
        assertThat(existing.getLatestObservation().getName()).isEqualTo("Balcony Arduino");
        assertThat(existing.getLatestObservation().getState()).isEqualTo(State.REGISTERED);
        assertThat(existing.getLastSeenAt()).isBetween(receivedAfter, Instant.now());
        assertThat(existing.getVersion()).isEqualTo(4L);
    }

    @Test
    void rejectsAnOlderObservationWithoutSaving() {
        var observation = observation();
        observation.setObservedAt(2000);
        var existing = mapper.toDocument(observation, Instant.EPOCH, 4L);
        var incoming = mapper.copyObservation(observation);
        incoming.setObservedAt(1000);
        when(states.findById(existing.getId())).thenReturn(Mono.just(existing));

        StepVerifier.create(service.processDeviceData(incoming))
                .expectErrorMessage("Stale device entry").verify(Duration.ofSeconds(1));

        verify(states, never()).save(any(DeviceStateDocument.class));
        assertThat(existing.getLatestObservation()).isEqualTo(observation);
        assertThat(existing.getLastSeenAt()).isEqualTo(Instant.EPOCH);
    }

    @Test
    void propagatesAnExistingDeviceSaveFailure() {
        var observation = observation();
        var existing = mapper.toDocument(observation, Instant.EPOCH, 4L);
        var failure = new IllegalStateException("MongoDB update failed");
        when(states.findById(existing.getId())).thenReturn(Mono.just(existing));
        when(states.save(existing)).thenReturn(Mono.error(failure));

        StepVerifier.create(service.processDeviceData(observation))
                .expectErrorMatches(error -> error == failure).verify(Duration.ofSeconds(1));
    }

    @Test
    void propagatesFailureFromTheSavePublisher() {
        var observation = observation();
        var failure = new IllegalStateException("MongoDB write failed");
        when(states.findById(new DeviceIdentity(observation.getDeviceId(), true))).thenReturn(Mono.empty());
        when(states.save(any(DeviceStateDocument.class))).thenReturn(Mono.error(failure));

        StepVerifier.create(service.processDeviceData(observation))
                .expectErrorMatches(error -> error == failure).verify(Duration.ofSeconds(1));
    }

    private DeviceData observation() {
        var observation = new DeviceData();
        observation.setDeviceId(UUID.randomUUID());
        observation.setEventId(UUID.randomUUID());
        observation.setState(State.UNNAMED);
        observation.setEventType("SOIL_MOISTURE_READING");
        observation.setMock(true);
        observation.setBatteryLevelPct(10);
        observation.setErrors(List.of());
        return observation;
    }
}
