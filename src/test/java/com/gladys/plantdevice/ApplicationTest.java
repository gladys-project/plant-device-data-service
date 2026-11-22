package com.gladys.plantdevice;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import com.mongodb.reactivestreams.client.MongoClient;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.config.location=classpath:/application-test.yml")
class ApplicationTest {
    private final WebTestClient web;
    @Autowired ApplicationContext context;

    @Autowired
    ApplicationTest(@LocalServerPort int port) {
        web = WebTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    @Test
    void startsWithoutReachableInfrastructureAndServesReactiveHealth() {
        var response = web.get().uri("/actuator/health").exchange()
                .expectStatus().isOk().returnResult(Map.class);
        StepVerifier.create(response.getResponseBody())
                .assertNext(body -> assertThat(body.get("status")).isEqualTo("UP"))
                .expectComplete().verify(Duration.ofSeconds(5));
    }

    @Test
    void exposesOnlyHealthAndInfo() {
        web.get().uri("/actuator/info").exchange().expectStatus().isOk();
        web.get().uri("/actuator/env").exchange().expectStatus().isNotFound();
        web.get().uri("/actuator/beans").exchange().expectStatus().isNotFound();
    }

    @Test
    void registersTheDeviceListDelegate() {
        assertThat(context.getBeansOfType(com.gladys.plantdevice.api.DevicesApiDelegate.class)).hasSize(1);
    }

    @Test
    void usesStandardReactiveMongoAndKafkaInfrastructureWithAnUnboundListener() {
        assertThat(context.getBeansOfType(ReactiveMongoTemplate.class)).hasSize(1);
        assertThat(context.getBeansOfType(MongoClient.class)).hasSize(1);
        assertThat(context.getBeansOfType(KafkaTemplate.class)).hasSize(1);
        assertThat(context.getBeansOfType(com.gladys.plantdevice.mapping.KafkaDeviceDataMapper.class)).hasSize(1);
        assertThat(context.getBeansOfType(org.apache.kafka.clients.admin.NewTopic.class)).isEmpty();
        assertThat(context.getBeansOfType(org.springframework.data.mongodb.core.MongoTemplate.class)).isEmpty();
        assertThat(context.getBean("onDeviceDataEvent")).isInstanceOf(java.util.function.Function.class);
        var bindings = context.getBean(BindingService.class);
        assertThat(bindings.getConsumerBindingNames()).isEmpty();
        assertThat(bindings.getProducerBindingNames()).isEmpty();
        assertThat(context.getBean(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class)
                .getListenerContainers()).isEmpty();
        assertThat(context.getClass().getSimpleName()).contains("ReactiveWebServer");
    }

    @Test
    void testConfigurationActuallyUsesIsolatedMongoAndKafkaAddresses() {
        assertThat(context.getBean(MongoClient.class).getClusterDescription().getServerDescriptions())
                .extracting(server -> server.getAddress().toString()).containsExactly("127.0.0.1:1");
        assertThat(context.getBean(org.springframework.boot.kafka.autoconfigure.KafkaProperties.class)
                .getBootstrapServers()).containsExactly("127.0.0.1:1");
    }
}
