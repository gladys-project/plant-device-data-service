package com.gladys.plantdevice.persistence;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import com.gladys.plantdevice.mapping.DeviceStateDocumentMapper;
import com.gladys.plantdevice.mapping.KafkaDeviceDataMapper;
import com.gladys.plantdevice.message.KafkaDeviceData;
import com.gladys.plantdevice.model.DeviceData;
import com.gladys.plantdevice.model.State;
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
        var identity = observation.getDeviceId();
        var seenAt = Instant.parse("2026-11-17T10:00:00Z");
        var state = new DeviceStateDocument(identity, observation, seenAt, null);
        var bson = new Document();

        converter.write(state, bson);

        assertThat(bson.get("_id")).isEqualTo(observation.getDeviceId());
        assertThat(bson.getDate("lastSeenAt")).isEqualTo(Date.from(seenAt));
        assertThat(converter.read(DeviceStateDocument.class, bson)).isEqualTo(state);
    }

    @Test
    void mapsObservationWithItsDeviceUuidAndTheSuppliedPersistenceMetadata() throws Exception {
        var data = observation();
        var seenAt = Instant.parse("2026-11-17T10:00:00Z");
        var mapper = Mappers.getMapper(DeviceStateDocumentMapper.class);

        var insert = mapper.toDocument(data, seenAt, null);
        var update = mapper.toDocument(data, seenAt, 7L);
        var bson = new Document();
        converter.write(insert, bson);

        assertThat(bson.get("_id")).isEqualTo(data.getDeviceId());
        assertThat(insert.getLatestObservation()).isEqualTo(data);
        assertThat(insert.getLastSeenAt()).isEqualTo(seenAt);
        assertThat(insert.getVersion()).isNull();
        assertThat(update.getVersion()).isEqualTo(7L);

        var savedObservation = insert.getLatestObservation();
        savedObservation.setName("Balcony Arduino");
        savedObservation.setState(State.REGISTERED);
        assertThat(data.getName()).isNull();
        assertThat(update.getLatestObservation()).isEqualTo(data);

        var namedBson = new Document();
        converter.write(insert, namedBson);
        assertThat(converter.read(DeviceStateDocument.class, namedBson)).isEqualTo(insert);
    }

    @Test
    void historyKeepsLiteralIdsSourceEventIdentityAndDiagnosticDetails() throws Exception {
        var data = observation();
        var history = new DiagnosticHistoryDocument("507f1f77bcf86cd799439011",
                data.getDeviceId(), data.getEventId(), data.getErrors().getFirst(),
                DiagnosticHistoryDocument.Change.OPENED, data.getObservedAt(), Instant.parse("2026-11-17T10:00:00Z"));
        var bson = new Document();

        converter.write(history, bson);

        assertThat(bson.get("_id")).isEqualTo("507f1f77bcf86cd799439011");
        assertThat(bson.get("sourceEventId")).isEqualTo(data.getEventId());
        assertThat(converter.read(DiagnosticHistoryDocument.class, bson)).isEqualTo(history);
    }


}
