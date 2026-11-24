package com.gladys.plantdevice.mapping;

import java.time.Instant;

import com.gladys.plantdevice.model.DeviceData;
import com.gladys.plantdevice.persistence.model.DeviceStateDocument;
import org.mapstruct.InjectionStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING,
        injectionStrategy = InjectionStrategy.CONSTRUCTOR,
        unmappedSourcePolicy = ReportingPolicy.ERROR,
        unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface DeviceStateDocumentMapper {
    /** Pass a null version for an insert and the loaded version for an update. */
    @Mapping(target = "deviceId", source = "data.deviceId")
    @Mapping(target = "latestObservation", source = "data")
    @Mapping(target = "lastSeenAt", source = "receivedAt")
    @Mapping(target = "version", source = "version")
    DeviceStateDocument toDocument(DeviceData data, Instant receivedAt, Long version);

    DeviceData copyObservation(DeviceData data);
}
