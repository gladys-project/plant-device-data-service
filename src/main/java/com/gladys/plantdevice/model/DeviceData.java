package com.gladys.plantdevice.model;

import java.util.List;
import java.util.UUID;

/** Internal input model; persistence and message processing are outside this skeleton. */
public record DeviceData(
        Integer schemaVersion,
        UUID eventId,
        String eventType,
        UUID deviceId,
        UUID bootId,
        Long sequence,
        Long uptimeMs,
        Long observedAt,
        String clockSource,
        Boolean mock,
        Integer batteryLevelPct,
        List<Diagnostic> errors) {
    public DeviceData {
        errors = errors == null ? null : List.copyOf(errors);
    }

    public record Diagnostic(String code, String component, String severity, String message) { }
}
