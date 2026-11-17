package com.gladys.plantdevice.service;

import java.time.Instant;
import java.util.List;

import com.gladys.plantdevice.model.DeviceData;
import com.gladys.plantdevice.persistence.model.DeviceIdentity;
import com.gladys.plantdevice.persistence.model.DeviceStateDocument;
import com.gladys.plantdevice.persistence.model.DiagnosticHistoryDocument;
import com.gladys.plantdevice.persistence.repository.DeviceStateRepository;
import com.gladys.plantdevice.persistence.repository.DiagnosticHistoryRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Slf4j
@Service
@AllArgsConstructor
public class PlantDeviceDataService {
    private final DeviceStateRepository deviceStateRepository;
    private final DiagnosticHistoryRepository diagnosticHistoryRepository;

    /** Structure only: processing and persistence orchestration are not implemented. */
    public Mono<Void> processDeviceData(DeviceData data) {
        return Mono.error(() -> new UnsupportedOperationException("Device data processing is not implemented"));
    }

    private Mono<DeviceStateDocument> loadCurrentState(DeviceIdentity identity) {
        throw new UnsupportedOperationException("State loading is not implemented");
    }

    private DeviceStateDocument buildUpdatedState(DeviceData incoming, DeviceStateDocument previous, Instant receivedAt) {
        throw new UnsupportedOperationException("State reconciliation is not implemented");
    }

    private List<DiagnosticHistoryDocument> detectDiagnosticChanges(
            DeviceData incoming, DeviceStateDocument previous, Instant receivedAt) {
        throw new UnsupportedOperationException("Diagnostic reconciliation is not implemented");
    }

    private Mono<Void> evaluateBattery(DeviceData incoming, DeviceStateDocument previous) {
        throw new UnsupportedOperationException("Battery evaluation is not implemented");
    }

    private Mono<Void> persistChanges(DeviceStateDocument state, List<DiagnosticHistoryDocument> changes) {
        throw new UnsupportedOperationException("Persistence orchestration is not implemented");
    }

    private Mono<Void> requestNotifications(DeviceStateDocument state, List<DiagnosticHistoryDocument> changes) {
        throw new UnsupportedOperationException("Notification requests are not implemented");
    }
}
