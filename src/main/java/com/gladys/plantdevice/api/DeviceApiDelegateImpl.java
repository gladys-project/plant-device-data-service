package com.gladys.plantdevice.api;

import com.gladys.plantdevice.api.model.DeviceRegistrationRequest;
import com.gladys.plantdevice.api.model.DeviceRegistrationResponse;
import com.gladys.plantdevice.api.model.DeviceSummary;
import com.gladys.plantdevice.service.PlantDeviceDataService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.NoSuchElementException;

@Service
@RequiredArgsConstructor
public class DeviceApiDelegateImpl implements DevicesApiDelegate {
    private final PlantDeviceDataService service;

    @Override
    public Mono<ResponseEntity<Flux<DeviceSummary>>> listDevices(ServerWebExchange exchange) {
        return Mono.just(ResponseEntity.ok(service.listDevices()));
    }

    @Override
    public Mono<ResponseEntity<DeviceRegistrationResponse>> registerDevice(Mono<DeviceRegistrationRequest> deviceRegistrationRequest,
                                                     ServerWebExchange exchange) {
        return deviceRegistrationRequest.flatMap(request ->
                service.registerDevice(request.getDeviceId(), request.getName()))
                .map(ResponseEntity::ok)
                .onErrorResume(NoSuchElementException.class, error -> Mono.just(ResponseEntity.notFound().build()));
    }
}
