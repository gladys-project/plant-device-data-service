package com.gladys.plantdevice.mapping;

import com.gladys.plantdevice.message.DeviceError;
import com.gladys.plantdevice.message.KafkaDeviceData;
import com.gladys.plantdevice.model.DeviceData;
import org.mapstruct.InjectionStrategy;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;
import org.mapstruct.ReportingPolicy;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING,
        injectionStrategy = InjectionStrategy.CONSTRUCTOR,
        unmappedSourcePolicy = ReportingPolicy.ERROR,
        unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface KafkaDeviceDataMapper {
    DeviceData toDeviceData(KafkaDeviceData source);

    DeviceData.Diagnostic toDiagnostic(DeviceError source);

    default Integer schemaVersion(KafkaDeviceData.SchemaVersionEnum version) {
        return version == null ? null : version.getValue();
    }
}
