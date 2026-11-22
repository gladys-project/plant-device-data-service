package com.gladys.plantdevice.api;

import com.gladys.plantdevice.api.model.DeviceSummary;
import com.gladys.plantdevice.service.PlantDeviceDataService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class DeviceApiDelegateImpl implements DevicesApiDelegate {
    private final PlantDeviceDataService service;

    @Override
    public Mono<ResponseEntity<Flux<DeviceSummary>>> listDevices(ServerWebExchange exchange) {
        return Mono.just(ResponseEntity.ok(service.listDevices()));
    }
}
