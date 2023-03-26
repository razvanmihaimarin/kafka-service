package com.nordcloud.kafkaservice.service;

import com.nordcloud.kafkaservice.mapper.ProductMapper;
import com.nordcloud.kafkaservice.model.dto.ProductDto;
import com.nordcloud.kafkaservice.model.entity.Product;
import com.nordcloud.kafkaservice.repository.ProductRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.mockito.Mockito.doThrow;

@ExtendWith({MockitoExtension.class, OutputCaptureExtension.class})
public class KafkaConsumerServiceTest {

    private static final int DEFAULT_PARTITION = 1;

    public static final String TEST_TOPIC = "test_topic";

    private static final Duration DEFAULT_VERIFY_TIMEOUT = Duration.ofSeconds(10);
    public static final String ID = "1";
    public static final String TEST_PRODUCT = "Test Product";
    public static final String TEST_DESCRIPTION = "Test Description";
    public static final double PRICE = 1D;

    @Mock
    private ProductRepository repository;

    @Mock
    private ProductMapper productMapper;

    @Mock
    private ReactiveKafkaConsumerTemplate<String, ProductDto> reactiveKafkaConsumerTemplate;

    private KafkaConsumerService consumerService;

    @BeforeEach
    public void setUp() {
        consumerService = new KafkaConsumerService(reactiveKafkaConsumerTemplate, repository, productMapper, 10);
    }

    @Test
    @SuppressWarnings("unchecked")
    void givenProductDto_whenConsumingRecord_shouldReturnFluxWithOneProduct(CapturedOutput capturedOutput) {
        // given
        Mockito.when(repository.saveAll(Mockito.any(Iterable.class))).thenReturn(Flux.just(buildProductDocument()));
        Mockito.when(productMapper.toProduct(Mockito.any(ProductDto.class))).thenReturn(buildProductDocument());
        ConsumerRecord<String, ProductDto> consumerRecord = new ConsumerRecord(TEST_TOPIC, DEFAULT_PARTITION, 0, null, buildTestProductDto());
        Mockito.when(reactiveKafkaConsumerTemplate.receiveAutoAck()).thenReturn(Flux.just(consumerRecord));

        // when then
        StepVerifier.create(consumerService.consumeProducts())
                .assertNext(productDocuments -> {
                            Assertions.assertTrue(capturedOutput.getAll().contains("Successfully processed events"));
                            Assertions.assertEquals(buildProductDocument(), productDocuments.get(0));
                        }
                )
                .thenCancel()
                .verify(DEFAULT_VERIFY_TIMEOUT);
    }

    @Test
    @SuppressWarnings("unchecked")
    void givenProductDto_whenConsumingRecordAndThrowingSQLRelatedException_shouldLogAndContinueProcessing(CapturedOutput capturedOutput) {
        // given
        Mockito.when(productMapper.toProduct(Mockito.any(ProductDto.class))).thenReturn(buildProductDocument());
        doThrow(new DataIntegrityViolationException("SQL Exception")).when(repository).saveAll(Mockito.any(Iterable.class));
        ConsumerRecord<String, ProductDto> consumerRecord = new ConsumerRecord(TEST_TOPIC, DEFAULT_PARTITION, 0, null, buildTestProductDto());
        Mockito.when(reactiveKafkaConsumerTemplate.receiveAutoAck()).thenReturn(Flux.just(consumerRecord));

        // when then
        StepVerifier.create(consumerService.consumeProducts())
                .assertNext(products -> {
                    Assertions.assertTrue(capturedOutput.getAll().contains("Error during product processing"));
                    Assertions.assertEquals(0, products.size());
                })
                .thenCancel()
                .verify();
    }

    private ProductDto buildTestProductDto() {
        return ProductDto.builder()
                .uuid(ID)
                .name(TEST_PRODUCT)
                .description(TEST_DESCRIPTION)
                .price(PRICE)
                .build();
    }

    private Product buildProductDocument() {
        Product product = new Product();
        product.setUuid(ID);
        product.setName(TEST_PRODUCT);
        product.setDescription(TEST_DESCRIPTION);
        product.setPrice(PRICE);

        return product;
    }


}
