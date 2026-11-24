package com.gladys.plantdevice.persistence.repository;

import java.util.UUID;

import com.gladys.plantdevice.persistence.model.DeviceStateDocument;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

public interface DeviceStateRepository extends ReactiveMongoRepository<DeviceStateDocument, UUID> {
}
