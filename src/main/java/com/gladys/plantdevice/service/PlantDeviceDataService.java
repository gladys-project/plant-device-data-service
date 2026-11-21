package com.gladys.plantdevice.service;

import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;

import com.gladys.plantdevice.mapping.DeviceStateDocumentMapper;
import com.gladys.plantdevice.model.DeviceData;
import com.gladys.plantdevice.model.State;
import com.gladys.plantdevice.persistence.model.DeviceIdentity;
import com.gladys.plantdevice.persistence.model.DeviceStateDocument;
import com.gladys.plantdevice.persistence.model.DiagnosticHistoryDocument;
import com.gladys.plantdevice.persistence.repository.DeviceStateRepository;
import com.gladys.plantdevice.persistence.repository.DiagnosticHistoryRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@Service
@AllArgsConstructor
public class PlantDeviceDataService {
    private final DeviceStateRepository deviceStateRepository;
    private final DiagnosticHistoryRepository diagnosticHistoryRepository;
    private final DeviceStateDocumentMapper documentMapper;

    /** Creates current state for a device identity when it is first observed. */
    public Mono<Void> processDeviceData(DeviceData data) {
        var identity = documentMapper.toIdentity(data);
        return deviceStateRepository.findById(identity)
                .switchIfEmpty(Mono.defer(() -> persistIncomingData(data)))
                .then();
    }

    public Flux<DiagnosticHistoryDocument> diagnosticHistoryDocument(UUID deviceId, int size){
        var deviceIdentity = DeviceIdentity
                .builder()
                .deviceId(deviceId)
                .mock(false)
                .build();
        return diagnosticHistoryRepository
                .findByDeviceOrderByRecordedAtDescIdAsc(deviceIdentity, Pageable.ofSize(size));
    }

    public Mono<Void> registerDevice(UUID deviceId, String name) {
        var deviceName = name.strip();
        return deviceStateRepository.findAllById_DeviceId(deviceId)
                .switchIfEmpty(Mono.error(() -> new NoSuchElementException("Device not found: " + deviceId)))
                .concatMap(document -> {
                    var observation = document.getLatestObservation();
                    observation.setName(deviceName);
                    observation.setState(State.REGISTERED);
                    return deviceStateRepository.save(document);
                })
                .then();
    }

    private Mono<DeviceStateDocument> persistIncomingData(DeviceData data) {
        var deviceDocument = documentMapper.toDocument(data, Instant.now(), null);
        log.info("Device saved with id: {}", data.getDeviceId());
        return deviceStateRepository.save(deviceDocument);
    }

}
