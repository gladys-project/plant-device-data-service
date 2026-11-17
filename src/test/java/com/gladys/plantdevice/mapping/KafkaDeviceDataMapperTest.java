package com.gladys.plantdevice.mapping;

import com.gladys.plantdevice.message.KafkaDeviceData;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.springframework.core.io.ClassPathResource;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaDeviceDataMapperTest {
    private final KafkaDeviceDataMapper mapper = Mappers.getMapper(KafkaDeviceDataMapper.class);
    private final JsonMapper json = JsonMapper.builder().build();

    private KafkaDeviceData inbound() throws Exception {
        try (var input = new ClassPathResource("openapi/examples/kafka-device-data.json").getInputStream()) {
            return json.readValue(input, KafkaDeviceData.class);
        }
    }

    @Test
    void mapsEveryInboundFieldWithoutChangingTheSchemaVersionOrDeviceIdentity() throws Exception {
        var source = inbound();

        var target = mapper.toDeviceData(source);

        assertThat(target.schemaVersion()).isEqualTo(1);
        assertThat(target.deviceId()).isEqualTo(source.getDeviceId());
        assertThat(json.readTree(json.writeValueAsString(target)))
                .isEqualTo(json.readTree(json.writeValueAsString(source)));
    }

    @Test
    void isolatesTheImmutableTargetFromMutableInboundDiagnostics() throws Exception {
        var source = inbound();
        var target = mapper.toDeviceData(source);
        var diagnostic = target.errors().getFirst();

        source.getErrors().getFirst().setMessage("Changed after mapping");
        source.getErrors().clear();

        assertThat(target.errors()).containsExactly(diagnostic);
        assertThat(diagnostic.message()).isEqualTo("Mock battery level is below the pump operating threshold.");
        assertThatThrownBy(() -> target.errors().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void preservesAnEmptyDiagnosticListAndTheOriginalObservationType() throws Exception {
        var source = inbound();
        source.getErrors().clear();
        source.setEventType("SOIL_TEMPERATURE_READING");

        var target = mapper.toDeviceData(source);

        assertThat(target.errors()).isEmpty();
        assertThat(target.eventType()).isEqualTo("SOIL_TEMPERATURE_READING");
        assertThat(source.getEventType()).isEqualTo(target.eventType());
    }
}
