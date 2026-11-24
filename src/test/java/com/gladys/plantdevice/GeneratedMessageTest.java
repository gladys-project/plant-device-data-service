package com.gladys.plantdevice;

import java.util.List;
import java.util.UUID;

import com.gladys.plantdevice.message.KafkaDeviceData;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.io.ClassPathResource;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import static org.assertj.core.api.Assertions.assertThat;

class GeneratedMessageTest {
    private static ValidatorFactory factory;
    private static Validator validator;
    private final JsonMapper json = JsonMapper.builder().build();

    @BeforeAll
    static void validation() {
        factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @AfterAll
    static void closeValidation() {
        factory.close();
    }

    private ObjectNode example() throws Exception {
        try (var input = new ClassPathResource("openapi/examples/kafka-device-data.json").getInputStream()) {
            return (ObjectNode) json.readTree(input);
        }
    }

    @Test
    void preservesTheProcessorOutputEnvelopeAndDiagnosticsWithoutSensorPayload() throws Exception {
        var original = example();
        var message = json.treeToValue(original, KafkaDeviceData.class);

        assertThat(message.getDeviceId()).isEqualTo(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertThat(message.getObservedAt()).isEqualTo(1794164428500L);
        assertThat(message.getErrors().getFirst().getCode()).isEqualTo("BAT_01");
        assertThat(validator.validate(message)).isEmpty();
        assertThat(original.has("payload")).isFalse();
        assertThat(json.readTree(json.writeValueAsString(message))).isEqualTo(original);
    }

    @Test
    void generatedConstraintsCascadeIntoDeviceDiagnostics() throws Exception {
        var message = json.treeToValue(example(), KafkaDeviceData.class);
        message.setDeviceId(null);
        message.setBatteryLevelPct(101);
        message.getErrors().getFirst().setCode("invalid code");
        message.getErrors().getFirst().setMessage(" ");

        assertThat(validator.validate(message)).extracting(v -> v.getPropertyPath().toString())
                .contains("deviceId", "batteryLevelPct", "errors[0].code", "errors[0].message");
    }

    @ParameterizedTest
    @ValueSource(strings = {"sequence", "uptimeMs", "observedAt"})
    void rejectsNegativeCountersAndTimestamps(String property) throws Exception {
        var original = example().put(property, -1L);
        var message = json.treeToValue(original, KafkaDeviceData.class);

        assertThat(validator.validate(message)).extracting(v -> v.getPropertyPath().toString()).contains(property);
    }

    @Test
    void rejectsNullDiagnosticArrayEntries() throws Exception {
        var message = json.treeToValue(example(), KafkaDeviceData.class);
        message.getErrors().add(null);

        assertThat(validator.validate(message)).extracting(v -> v.getPropertyPath().toString())
                .contains("errors[1].<list element>");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 100})
    void acceptsBatteryBoundariesAndAnEmptyDiagnosticsList(int battery) throws Exception {
        var message = json.treeToValue(example(), KafkaDeviceData.class);
        message.setBatteryLevelPct(battery);
        message.setErrors(List.of());

        assertThat(validator.validate(message)).isEmpty();
    }
}
