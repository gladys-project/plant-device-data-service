package com.gladys.plantdevice.mapping;

import java.util.UUID;

import com.gladys.plantdevice.api.model.DeviceRegistrationResponse;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface DeviceRegistrationMapper {
    @Mapping(target = "deviceId", source = "deviceId")
    @Mapping(target = "name", source = "name")
    @Mapping(target = "state", constant = "REGISTERED")
    DeviceRegistrationResponse toResponse(UUID deviceId, String name);
}
