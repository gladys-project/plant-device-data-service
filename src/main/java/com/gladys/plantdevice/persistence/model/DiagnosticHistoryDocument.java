package com.gladys.plantdevice.persistence.model;

import java.time.Instant;
import java.util.UUID;

import com.gladys.plantdevice.model.DeviceData;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

@Document("device_diagnostic_history")
@CompoundIndex(name = "device_history", def = "{'deviceId': 1, 'recordedAt': -1, '_id': 1}")
@Data
@AllArgsConstructor
public class DiagnosticHistoryDocument {
    @MongoId
    private final String id;
    private UUID deviceId;
    private UUID sourceEventId;
    private DeviceData.Diagnostic diagnostic;
    private Change change;
    private Long observedAt;
    private Instant recordedAt;

    public enum Change { OPENED, UPDATED, RESOLVED }
}
