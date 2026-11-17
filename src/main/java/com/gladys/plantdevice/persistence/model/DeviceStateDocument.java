package com.gladys.plantdevice.persistence.model;

import java.time.Instant;

import com.gladys.plantdevice.model.DeviceData;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("device_states")
public record DeviceStateDocument(
        @Id DeviceIdentity id,
        DeviceData latestObservation,
        Instant lastSeenAt,
        @Version Long version) { }
