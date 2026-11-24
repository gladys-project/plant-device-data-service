package com.gladys.plantdevice.mapping;

import com.gladys.plantdevice.message.KafkaDeviceData;
import com.gladys.plantdevice.model.State;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.springframework.core.io.ClassPathResource;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

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

        assertThat(target.getSchemaVersion()).isEqualTo(1);
        assertThat(target.getDeviceId()).isEqualTo(source.getDeviceId());
        assertThat(target.getName()).isNull();
        assertThat(target.getState()).isEqualTo(State.UNNAMED);
        var expected = (ObjectNode) json.readTree(json.writeValueAsString(source));
        expected.putNull("name");
        expected.put("state", "UNNAMED");
        expected.putNull("supplyVoltageMv");
        assertThat(json.readTree(json.writeValueAsString(target))).isEqualTo(expected);
    }

    @Test
    void preservesMeasuredSupplyWithoutABatteryPercentage() throws Exception {
        var input = inbound();
        input.setBatteryLevelPct(null);
        input.setSupplyVoltageMv(4750);

        var observation = mapper.toDeviceData(input);

        assertThat(observation.getBatteryLevelPct()).isNull();
        assertThat(observation.getSupplyVoltageMv()).isEqualTo(4750);
    }

    @Test
    void copiesInboundDiagnosticsIntoAnUnmodifiableList() throws Exception {
        var source = inbound();
        var target = mapper.toDeviceData(source);
        var diagnostic = target.getErrors().getFirst();

        source.getErrors().getFirst().setMessage("Changed after mapping");
        source.getErrors().clear();

        assertThat(target.getErrors()).containsExactly(diagnostic);
        assertThat(diagnostic.message()).isEqualTo("Battery level is below the pump operating threshold.");
        assertThatThrownBy(() -> target.getErrors().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void preservesAnEmptyDiagnosticListAndTheOriginalObservationType() throws Exception {
        var source = inbound();
        source.getErrors().clear();
        source.setEventType("SOIL_TEMPERATURE_READING");

        var target = mapper.toDeviceData(source);

        assertThat(target.getErrors()).isEmpty();
        assertThat(target.getEventType()).isEqualTo("SOIL_TEMPERATURE_READING");
        assertThat(source.getEventType()).isEqualTo(target.getEventType());
    }

    @Test
    void preservesDistinctCriticalConditionsAcrossSuccessiveSnapshots() throws Exception {
        var observation = (ObjectNode) json.readTree(json.writeValueAsString(inbound()));
        var errors = observation.putArray("errors");
        errors.addObject().put("code", "SEN_01").put("component", "SOIL_SENSOR_A")
                .put("severity", "CRITICAL").put("message", "Sensor read failed.");
        errors.addObject().put("code", "PMP_01").put("component", "PUMP_A")
                .put("severity", "CRITICAL").put("message", "Pump activation failed.");

        var first = mapper.toDeviceData(json.treeToValue(observation, KafkaDeviceData.class));
        assertThat(json.readTree(json.writeValueAsString(first)).get("errors")).isEqualTo(errors);

        errors.remove(0);
        var next = mapper.toDeviceData(json.treeToValue(observation, KafkaDeviceData.class));
        var nextErrors = json.readTree(json.writeValueAsString(next)).get("errors");
        assertThat(nextErrors).isEqualTo(errors);
        assertThat(nextErrors.size()).isEqualTo(1);
        assertThat(nextErrors.get(0).get("code").asString()).isEqualTo("PMP_01");
        assertThat(nextErrors.get(0).get("severity").asString()).isEqualTo("CRITICAL");
    }
}
