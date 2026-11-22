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
    DeviceSummary toSummary(DeviceStateDocument document);

    default OffsetDateTime toUtc(Instant timestamp) {
        return timestamp == null ? null : timestamp.atOffset(ZoneOffset.UTC);
    }
}
