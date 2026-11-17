package com.gladys.plantdevice.persistence.repository;

import com.gladys.plantdevice.persistence.model.DeviceIdentity;
import com.gladys.plantdevice.persistence.model.DeviceStateDocument;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

public interface DeviceStateRepository extends ReactiveMongoRepository<DeviceStateDocument, DeviceIdentity> { }
