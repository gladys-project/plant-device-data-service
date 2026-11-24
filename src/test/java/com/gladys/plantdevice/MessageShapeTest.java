package com.gladys.plantdevice;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class MessageShapeTest {
    @Test
    void messageModelsExposeDeviceDataWithoutAClassificationFlag() {
        for (Class<?> type : List.of(com.gladys.plantdevice.message.KafkaDeviceData.class, com.gladys.plantdevice.model.DeviceData.class, com.gladys.plantdevice.api.model.DeviceSummary.class)) {
            assertThat(Arrays.stream(type.getDeclaredFields()).map(Field::getName).toList())
                    .as(type.getSimpleName()).doesNotContain("mock");
        }
    }

    @Test
    void examplesContainNoClassificationFlag() throws Exception {
        var json = JsonMapper.builder().build();
        for (String filename : List.of("kafka-device-data.json")) {
            var document = json.readTree(Files.readString(Path.of("src/main/resources/openapi/examples", filename)));
            assertThat(document.has("mock")).as(filename).isFalse();
        }
    }
}
