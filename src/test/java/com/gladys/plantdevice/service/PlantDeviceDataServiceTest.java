package com.gladys.plantdevice.service;

import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.gladys.plantdevice.model.DeviceData;
import com.gladys.plantdevice.persistence.repository.DeviceStateRepository;
import com.gladys.plantdevice.persistence.repository.DiagnosticHistoryRepository;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class PlantDeviceDataServiceTest {
    @Test
    void exposesOnePublicProcessingEntrypointAndKeepsOtherStagesPrivate() throws Exception {
        var methods = Arrays.stream(PlantDeviceDataService.class.getDeclaredMethods())
                .filter(method -> !method.isSynthetic()).toList();
        assertThat(methods.stream().filter(method -> Modifier.isPublic(method.getModifiers()))
                .map(method -> method.getName())).containsExactly("processDeviceData");
        assertThat(methods.stream().filter(method -> !Modifier.isPublic(method.getModifiers())))
                .allSatisfy(method -> assertThat(Modifier.isPrivate(method.getModifiers())).isTrue());
        assertThat(PlantDeviceDataService.class.getMethod("processDeviceData", DeviceData.class).getReturnType())
                .isEqualTo(reactor.core.publisher.Mono.class);
    }

    @Test
    void unimplementedProcessingDoesNotWriteOrSilentlyAcceptAnObservation() {
        var states = mock(DeviceStateRepository.class);
        var history = mock(DiagnosticHistoryRepository.class);
        var service = new PlantDeviceDataService(states, history);
        var observation = new DeviceData(1, UUID.randomUUID(), "SOIL_MOISTURE_READING", UUID.randomUUID(),
                UUID.randomUUID(), 1L, 1000L, 1794164428500L, "MANUAL_TEST", true, 10, List.of());

        var result = service.processDeviceData(observation);

        verifyNoInteractions(states, history);
        StepVerifier.create(result).expectError(UnsupportedOperationException.class).verify(Duration.ofSeconds(1));
        verifyNoInteractions(states, history);
    }
}
