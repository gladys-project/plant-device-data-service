package com.gladys.plantdevice.api;

import java.util.UUID;

import com.gladys.plantdevice.api.model.DeviceRegistrationRequest;
import com.gladys.plantdevice.api.model.DeviceRegistrationResponse;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.core.ResolvableType;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeviceRegistrationContractTest {
    private static ValidatorFactory factory;
    private static Validator validator;

    @BeforeAll
    static void validation() {
        factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @AfterAll
    static void closeValidation() {
        factory.close();
    }

    @Test
    void acceptsADeviceUuidAndAssignedName() {
        assertThat(validator.validate(request())).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t", "\n"})
    void rejectsBlankNames(String name) {
        assertThat(validator.validate(request().name(name)))
                .extracting(violation -> violation.getPropertyPath().toString()).contains("name");
    }

    @Test
    void requiresADeviceIdAndName() {
        assertThat(validator.validate(new DeviceRegistrationRequest()))
                .extracting(violation -> violation.getPropertyPath().toString())
                .contains("deviceId", "name");
    }

    @Test
    void rejectsMalformedDeviceUuidsWhenReadingJson() {
        var json = JsonMapper.builder().build();
        assertThatThrownBy(() -> json.readValue(
                "{\"deviceId\":\"invalid\",\"name\":\"Balcony Arduino\"}", DeviceRegistrationRequest.class))
                .isInstanceOf(JacksonException.class);
    }

    @Test
    void requiresTheDeviceIdNameAndRegisteredStateInTheResponse() {
        assertThat(validator.validate(new DeviceRegistrationResponse()))
                .extracting(violation -> violation.getPropertyPath().toString())
                .contains("deviceId", "name", "state");
        var response = new DeviceRegistrationResponse()
                .deviceId(request().getDeviceId())
                .name("Balcony Arduino")
                .state(DeviceRegistrationResponse.StateEnum.REGISTERED);
        assertThat(validator.validate(response)).isEmpty();
    }

    @Test
    void generatesAReactiveDelegateWithARegistrationResponseBody() throws Exception {
        var method = DevicesApiDelegate.class.getMethod("registerDevice", Mono.class, ServerWebExchange.class);
        var response = ResolvableType.forMethodReturnType(method);
        assertThat(response.resolve()).isEqualTo(Mono.class);
        assertThat(response.getGeneric(0).resolve()).isEqualTo(org.springframework.http.ResponseEntity.class);
        assertThat(response.getGeneric(0).getGeneric(0).resolve()).isEqualTo(DeviceRegistrationResponse.class);
    }

    private DeviceRegistrationRequest request() {
        return new DeviceRegistrationRequest()
                .deviceId(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"))
                .name("Balcony Arduino");
    }
}
