package com.gladys.plantdevice.persistence.model;

import java.time.Instant;
import java.util.UUID;

import com.gladys.plantdevice.model.DeviceData;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

@Document("device_diagnostic_history")
@CompoundIndex(name = "device_history", def = "{'device': 1, 'recordedAt': -1, '_id': 1}")
public record DiagnosticHistoryDocument(
        @MongoId String id,
        DeviceIdentity device,
        UUID sourceEventId,
        DeviceData.Diagnostic diagnostic,
        Change change,
        Long observedAt,
        Instant recordedAt) {
    public enum Change { OPENED, UPDATED, RESOLVED }
}
