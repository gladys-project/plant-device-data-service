package com.gladys.plantdevice.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import lombok.Data;

/** Mutable device data used inside the service. */
@Data
public class DeviceData {
    private Integer schemaVersion;
    private UUID eventId;
    private String name;
    private State state;
    private String eventType;
    private UUID deviceId;
    private UUID bootId;
    private Long sequence;
    private Long uptimeMs;
    private long observedAt;
    private String clockSource;
    private Integer batteryLevelPct;
    private Integer supplyVoltageMv;
    private List<Diagnostic> errors;

    public void setErrors(List<Diagnostic> errors) {
        this.errors = errors == null ? null : List.copyOf(errors);
    }

    public record Diagnostic(String code, String component, String severity, String message) { }
}
