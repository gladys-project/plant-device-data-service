package com.gladys.plantdevice.mapping;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import com.gladys.plantdevice.api.model.DeviceSummary;
import com.gladys.plantdevice.persistence.model.DeviceStateDocument;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface DeviceSummaryMapper {
    @Mapping(target = "deviceId", source = "id.deviceId")
    @Mapping(target = "mock", source = "id.mock")
    @Mapping(target = "name", source = "latestObservation.name")
    @Mapping(target = "state", source = "latestObservation.state", defaultValue = "UNNAMED")
    @Mapping(target = "schemaVersion", source = "latestObservation.schemaVersion")
    @Mapping(target = "eventId", source = "latestObservation.eventId")
    @Mapping(target = "eventType", source = "latestObservation.eventType")
    @Mapping(target = "bootId", source = "latestObservation.bootId")
    @Mapping(target = "sequence", source = "latestObservation.sequence")
    @Mapping(target = "uptimeMs", source = "latestObservation.uptimeMs")
    @Mapping(target = "observedAt", source = "latestObservation.observedAt")
    @Mapping(target = "clockSource", source = "latestObservation.clockSource")
    @Mapping(target = "batteryLevelPct", source = "latestObservation.batteryLevelPct")
    @Mapping(target = "supplyVoltageMv", source = "latestObservation.supplyVoltageMv")
    @Mapping(target = "errors", source = "latestObservation.errors")
    DeviceSummary toSummary(DeviceStateDocument document);

    default OffsetDateTime toUtc(Instant timestamp) {
        return timestamp == null ? null : timestamp.atOffset(ZoneOffset.UTC);
    }
}
