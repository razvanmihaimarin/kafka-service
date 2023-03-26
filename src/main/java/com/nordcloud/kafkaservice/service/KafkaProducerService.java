package com.nordcloud.kafkaservice.service;

import com.nordcloud.kafkaservice.model.dto.ProductDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.stream.IntStream;

@Service
@Slf4j
public class KafkaProducerService {
    public static final String PRODUCT = "Prod-";

    private final ReactiveKafkaProducerTemplate<String, ProductDto> reactiveKafkaProducerTemplate;

    @Value(value = "${product.topic}")
    private String topic;

    public KafkaProducerService(ReactiveKafkaProducerTemplate<String, ProductDto> reactiveKafkaProducerTemplate) {
        this.reactiveKafkaProducerTemplate = reactiveKafkaProducerTemplate;
    }

    public void send(ProductDto productDto) {
        log.info("Sending to topic={}, {}={},", topic, ProductDto.class.getSimpleName(), productDto);
        reactiveKafkaProducerTemplate.send(topic, productDto)
                .doOnSuccess(senderResult -> log.info("Sent product {} offset : {}", productDto,
                        senderResult.recordMetadata().offset()))
                .subscribe();
    }

    @EventListener(ApplicationStartedEvent.class)
    public void produce() {
        IntStream.range(0, 100)
                .forEach(value -> {
                    String uuid = UUID.randomUUID().toString();
                    ProductDto productDto = ProductDto.builder()
                            .uuid(uuid)
                            .name(PRODUCT + uuid)
                            .description(PRODUCT + uuid)
                            .price(Math.floor(Math.random() * (100 + 1) + 0.1))
                            .build();
                    send(productDto);
                });
    }
}
