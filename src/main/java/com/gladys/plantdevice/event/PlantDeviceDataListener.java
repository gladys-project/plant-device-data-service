package com.gladys.plantdevice.event;

import com.gladys.plantdevice.mapping.KafkaDeviceDataMapper;
import com.gladys.plantdevice.message.KafkaDeviceData;
import com.gladys.plantdevice.service.PlantDeviceDataService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.util.function.Function;

@Slf4j
@Service
@AllArgsConstructor
public class PlantDeviceDataListener {

    private final ObjectMapper mapper;
    private final KafkaDeviceDataMapper kafkaDeviceDataMapper;
    private final PlantDeviceDataService service;

    @Bean
    public Function<Flux<Message<String>>, Mono<Void>> onDeviceDataEvent() {
        return data -> data
                .flatMap(msg -> processMessage(msg)
                        .onErrorResume(err -> {
                            sendToDlq(msg, err);
                            return Mono.empty();
                        })
                ).then();

    }

    private Mono<Void> processMessage(Message<String> message) {
        var rawDeviceData = extractData(message);
        var deviceDAta = kafkaDeviceDataMapper.toDeviceData(rawDeviceData);
        service.processDeviceData(deviceDAta);
        return Mono.empty();
    }

    private void sendToDlq(Message<String> message, Throwable error) {

    }

    private KafkaDeviceData extractData(Message<String> message) {
        try {
            return mapper.readValue(message.getPayload(), KafkaDeviceData.class);
        } catch (JacksonException e) {
            log.error("Error in parsing message");
            throw new RuntimeException(e);
        }
    }
}
