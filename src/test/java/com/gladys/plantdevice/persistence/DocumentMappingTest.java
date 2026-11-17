package com.gladys.plantdevice.persistence;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import com.gladys.plantdevice.mapping.KafkaDeviceDataMapper;
import com.gladys.plantdevice.message.KafkaDeviceData;
import com.gladys.plantdevice.model.DeviceData;
import com.gladys.plantdevice.persistence.model.DeviceIdentity;
import com.gladys.plantdevice.persistence.model.DeviceStateDocument;
import com.gladys.plantdevice.persistence.model.DiagnosticHistoryDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class DocumentMappingTest {
    private MappingMongoConverter converter;

    @BeforeEach
    void mapping() {
        var conversions = MongoCustomConversions.create(adapter -> { });
        var context = new MongoMappingContext();
        context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
        context.afterPropertiesSet();
        converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
        converter.setCustomConversions(conversions);
        converter.afterPropertiesSet();
    }

    private DeviceData observation() throws Exception {
        try (var input = new ClassPathResource("openapi/examples/kafka-device-data.json").getInputStream()) {
            var inbound = JsonMapper.builder().build().readValue(input, KafkaDeviceData.class);
            return Mappers.getMapper(KafkaDeviceDataMapper.class).toDeviceData(inbound);
        }
    }

    @Test
    void roundTripsDeviceIdentityObservationAndServerTimeWithoutChangingTheInputModel() throws Exception {
        var observation = observation();
        var identity = new DeviceIdentity(observation.deviceId(), true);
        var seenAt = Instant.parse("2026-11-17T10:00:00Z");
        var state = new DeviceStateDocument(identity, observation, seenAt, null);
        var bson = new Document();

        converter.write(state, bson);

        assertThat(bson.get("_id", Document.class).get("deviceId")).isEqualTo(observation.deviceId());
        assertThat(bson.get("_id", Document.class).getBoolean("mock")).isTrue();
        assertThat(bson.getDate("lastSeenAt")).isEqualTo(Date.from(seenAt));
        assertThat(converter.read(DeviceStateDocument.class, bson)).isEqualTo(state);
    }

    @Test
    void historyKeepsLiteralIdsSourceEventIdentityAndDiagnosticDetails() throws Exception {
        var data = observation();
        var history = new DiagnosticHistoryDocument("507f1f77bcf86cd799439011",
                new DeviceIdentity(data.deviceId(), true), data.eventId(), data.errors().getFirst(),
                DiagnosticHistoryDocument.Change.OPENED, data.observedAt(), Instant.parse("2026-11-17T10:00:00Z"));
        var bson = new Document();

        converter.write(history, bson);

        assertThat(bson.get("_id")).isEqualTo("507f1f77bcf86cd799439011");
        assertThat(bson.get("sourceEventId")).isEqualTo(data.eventId());
        assertThat(converter.read(DiagnosticHistoryDocument.class, bson)).isEqualTo(history);
    }

    @Test
    void aMockIdentityCannotEqualTheRealDeviceIdentity() {
        var id = UUID.randomUUID();
        assertThat(new DeviceIdentity(id, false)).isNotEqualTo(new DeviceIdentity(id, true));
    }
}
