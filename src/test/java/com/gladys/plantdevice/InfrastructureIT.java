package com.gladys.plantdevice;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.mongodb.MongoDBContainer;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "spring.config.location=classpath:/application-test.yml",
        "spring.kafka.admin.auto-create=true",
        "spring.kafka.admin.fail-fast=true",
        "spring.kafka.admin.operation-timeout=10s",
        "management.health.mongodb.enabled=true"
})
@Import(InfrastructureIT.TopicConfiguration.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class InfrastructureIT {
    private static final String TOPIC = "plant-device-data-infrastructure-it";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .waitingFor(Wait.forLogMessage(".*Kafka Server started.*\\n", 1))
            .withStartupTimeout(Duration.ofSeconds(90));

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:8.0.17")
            .withReplicaSet()
            .waitingFor(Wait.forLogMessage(".*Waiting for connections.*\\n", 1))
            .withStartupTimeout(Duration.ofSeconds(90));

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry properties) {
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        properties.add("spring.mongodb.uri", () -> MONGO.getReplicaSetUrl("plant_device_it"));
        properties.add("spring.cloud.stream.bindings.onDeviceDataEvent-in-0.destination", () -> TOPIC);
    }

    @Autowired KafkaTemplate<String, byte[]> kafka;
    @Autowired ConsumerFactory<String, byte[]> consumers;
    @Autowired ReactiveMongoTemplate mongo;

    @Test
    @Timeout(40)
    void configuredKafkaInfrastructurePreservesTheDeviceKeyAndInboundPayload() throws Exception {
        byte[] payload;
        try (var input = new ClassPathResource("openapi/examples/kafka-device-data.json").getInputStream()) {
            payload = input.readAllBytes();
        }
        var consumer = consumers.createConsumer("plant-device-data-it", "plant-device-data-it", "-1");
        try {
            var partition = new TopicPartition(TOPIC, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            kafka.send(TOPIC, "550e8400-e29b-41d4-a716-446655440000", payload).get(10, TimeUnit.SECONDS);

            var records = new ArrayList<ConsumerRecord<String, byte[]>>();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (records.isEmpty() && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach(records::add);
            }
            assertThat(records).hasSize(1);
            assertThat(records.getFirst().key()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
            assertThat(records.getFirst().value()).isEqualTo(payload);
        } finally {
            consumer.close(Duration.ofSeconds(3));
        }
    }

    @Test
    @Timeout(20)
    void configuredReactiveMongoTemplateCanWriteReadAndRemoveATestDocument() {
        String id = UUID.randomUUID().toString();
        String collection = "infrastructure_probe";
        var roundTrip = mongo.insert(new Document("_id", id).append("probe", "connectivity"), collection)
                .then(mongo.findById(id, Document.class, collection))
                .flatMap(found -> mongo.remove(Query.query(Criteria.where("_id").is(id)), collection)
                        .thenReturn(found));

        StepVerifier.create(roundTrip)
                .assertNext(found -> {
                    assertThat(found.getString("_id")).isEqualTo(id);
                    assertThat(found.getString("probe")).isEqualTo("connectivity");
                })
                .expectComplete().verify(Duration.ofSeconds(15));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class TopicConfiguration {
        @Bean
        NewTopic testDeviceDataTopic() {
            return TopicBuilder.name(TOPIC).partitions(1).replicas(1).build();
        }
    }
}
