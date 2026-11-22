package com.gladys.plantdevice.api;

import java.time.Instant;
import java.util.UUID;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webflux.test.autoconfigure.WebFluxTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

import static org.mockito.Mockito.when;

@WebFluxTest(DevicesApiController.class)
@Import({DeviceApiDelegateImpl.class, PlantDeviceDataService.class,
        DeviceStateDocumentMapperImpl.class, DeviceSummaryMapperImpl.class})
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
    void returnsAnEmptyArrayWhenThereAreNoSavedDevices() {
        when(states.findAll()).thenReturn(Flux.empty());

        web.get().uri("/devices").accept(MediaType.APPLICATION_JSON).exchange()
                .expectStatus().isOk()
                .expectBody().json("[]");
    }
}
