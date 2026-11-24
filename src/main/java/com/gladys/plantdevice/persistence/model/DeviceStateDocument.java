package com.gladys.plantdevice.persistence.model;

import java.time.Instant;
import java.util.UUID;

import com.gladys.plantdevice.model.DeviceData;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.mapping.Document;

@Document("device_states")
@Data
@AllArgsConstructor
public class DeviceStateDocument {
    @Id
    private final UUID deviceId;

    private DeviceData latestObservation;
    private Instant lastSeenAt;

    @Version
    private Long version;
}
