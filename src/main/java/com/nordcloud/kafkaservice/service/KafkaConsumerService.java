package com.nordcloud.kafkaservice.service;

import com.nordcloud.kafkaservice.mapper.ProductMapper;
import com.nordcloud.kafkaservice.model.dto.ProductDto;
import com.nordcloud.kafkaservice.model.entity.Product;
import com.nordcloud.kafkaservice.repository.ProductRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class KafkaConsumerService {

    private final ReactiveKafkaConsumerTemplate<String, ProductDto> reactiveKafkaConsumerTemplate;
    private final ProductRepository productRepository;
    private final ProductMapper productMapper;

    private final int bufferSize;

    public KafkaConsumerService(ReactiveKafkaConsumerTemplate<String, ProductDto> reactiveKafkaConsumerTemplate,
                                ProductRepository productRepository,
                                ProductMapper productMapper,
                                @Value("${bufferSize}") int bufferSize) {
        this.reactiveKafkaConsumerTemplate = reactiveKafkaConsumerTemplate;
        this.productRepository = productRepository;
        this.productMapper = productMapper;
        this.bufferSize = bufferSize;
    }

    @EventListener(ApplicationStartedEvent.class)
    public Disposable consume() {
        return consumeProducts().subscribe();
    }

    public Flux<List<Product>> consumeProducts(){
        return reactiveKafkaConsumerTemplate
                .receiveAutoAck()
                .doOnError(error -> log.error("Error receiving event, will retry", error)) // log error in case of receiving issues
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMinutes(1))) // retry if there are receiving issues
                .doOnNext(consumerRecord -> log.info("Processing record with key={}, value={} from topic={}, offset={}.",
                        consumerRecord.key(),
                        consumerRecord.value(),
                        consumerRecord.topic(),
                        consumerRecord.offset())
                ) // log processing started
                .map(ConsumerRecord::value)
                .map(productMapper::toProduct) // convert to entity - product
                .buffer(bufferSize) // buffer up to bufferSize
                .publishOn(Schedulers.boundedElastic()) // delegate following processing to different worker thread pool
                .doOnNext(products -> {
                    productRepository.saveAll(products).subscribe();
                    log.info("Successfully processed events: uuids - {}", products.stream().map(Product::getUuid)
                            .collect(Collectors.joining(", ")));
                }) // save products to database and log
                .doOnError(throwable -> log.error("Error during product processing. Details : {}", throwable.getMessage()))
                .onErrorResume(ex -> Flux.just(Collections.emptyList())); // move processing forward if errors appear in processing
    }

}
