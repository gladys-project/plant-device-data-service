package com.gladys.plantdevice.persistence.repository;

import java.util.UUID;

import com.gladys.plantdevice.persistence.model.DeviceIdentity;
import com.gladys.plantdevice.persistence.model.DeviceStateDocument;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;

public interface DeviceStateRepository extends ReactiveMongoRepository<DeviceStateDocument, DeviceIdentity> {
    Flux<DeviceStateDocument> findAllById_DeviceId(UUID deviceId);
}
