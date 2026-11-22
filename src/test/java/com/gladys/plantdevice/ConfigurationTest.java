package com.gladys.plantdevice;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.mongodb.autoconfigure.MongoProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.function.context.FunctionProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.integration.config.EnableIntegration;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withPropertyValues("spring.config.location=classpath:/application.yml")
            .withUserConfiguration(BoundProperties.class);

    @Test
    void declaresTheRenamedApplicationAndInactiveDeviceDataInput() {
        runner.run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ctx.getEnvironment().getProperty("spring.application.name"))
                    .isEqualTo("plant-device-data-service");
            var bindings = ctx.getBean(BindingServiceProperties.class);
            var input = bindings.getBindingProperties("onDeviceDataEvent-in-0");
            assertThat(input.getDestination()).isEqualTo("gladys.device.data.v1");
            assertThat(input.getGroup()).isEqualTo("gladys.plant-device-data-service.v1");
            assertThat(input.getContentType()).isEqualTo("application/json");
            assertThat(bindings.getConsumerProperties("onDeviceDataEvent-in-0").isAutoStartup()).isFalse();
            assertThat(ctx.getBean(FunctionProperties.class).getDefinition()).isEqualTo("onDeviceDataEvent");
            assertThat(ctx.getEnvironment().getProperty("spring.cloud.stream.function.autodetect", Boolean.class))
                    .isFalse();
            assertThat(ctx.getBean(KafkaProperties.class).getAdmin().isAutoCreate()).isFalse();
            assertThat(ctx.getEnvironment().getProperty("spring.cloud.stream.kafka.binder.auto-create-topics", Boolean.class))
                    .isFalse();
            assertThat(ctx.getEnvironment().getProperty("spring.cloud.stream.kafka.binder.auto-add-partitions", Boolean.class))
                    .isFalse();
            assertThat(new com.mongodb.ConnectionString(ctx.getBean(MongoProperties.class).getUri()).getDatabase())
                    .isEqualTo("plant_device_data");
        });
    }

    @Test
    void bindsEnvironmentOverridesWithoutDuplicatingBrokerSettings() {
        runner.withInitializer(ctx -> ctx.getEnvironment().getPropertySources().addFirst(
                new SystemEnvironmentPropertySource("systemEnvironment", Map.of(
                        "SPRING_KAFKA_BOOTSTRAPSERVERS", "kafka.example:29092",
                        "SPRING_MONGODB_URI", "mongodb://mongo.example:27018/custom_device_data",
                        "DEVICE_DATA_TOPIC", "gladys.device.data.test.v1",
                        "PLANT_DEVICE_DATA_GROUP", "plant-device-data-test"))))
                .run(ctx -> {
                    assertThat(ctx).hasNotFailed();
                    assertThat(ctx.getBean(KafkaProperties.class).getBootstrapServers())
                            .containsExactly("kafka.example:29092");
                    assertThat(ctx.getEnvironment().getProperty("spring.cloud.stream.kafka.binder.brokers"))
                            .isEqualTo("kafka.example:29092");
                    var input = ctx.getBean(BindingServiceProperties.class).getBindingProperties("onDeviceDataEvent-in-0");
                    assertThat(input.getDestination()).isEqualTo("gladys.device.data.test.v1");
                    assertThat(input.getGroup()).isEqualTo("plant-device-data-test");
                    assertThat(ctx.getBean(MongoProperties.class).getUri())
                            .isEqualTo("mongodb://mongo.example:27018/custom_device_data");
                    assertThat(new com.mongodb.ConnectionString(ctx.getBean(MongoProperties.class).getUri()).getDatabase())
                            .isEqualTo("custom_device_data");
                });
    }

    @Test
    void deployedProfileChecksMongoHealthAndStartsDeviceDataConsumption() {
        runner.withPropertyValues("spring.profiles.active=deployed").run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ctx.getEnvironment().getProperty("management.health.mongodb.enabled", Boolean.class)).isTrue();
            assertThat(ctx.getBean(FunctionProperties.class).getDefinition()).isEqualTo("onDeviceDataEvent");
            assertThat(ctx.getBean(BindingServiceProperties.class).getConsumerProperties("onDeviceDataEvent-in-0").isAutoStartup())
                    .isTrue();
        });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableIntegration
    @EnableConfigurationProperties({KafkaProperties.class, MongoProperties.class,
            BindingServiceProperties.class, FunctionProperties.class})
    static class BoundProperties { }
}
