package com.nordcloud.kafkaservice;

import com.nordcloud.kafkaservice.mapper.ProductMapper;
import com.nordcloud.kafkaservice.model.dto.ProductDto;
import com.nordcloud.kafkaservice.repository.ProductRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(topics = ReactiveKafkaIntegrationTest.TEST_TOPIC, partitions = 1)
public class ReactiveKafkaIntegrationTest {

    public static final String TEST_TOPIC = "testTopic";
    private static final ProductDto DEFAULT_VALUE = ProductDto.builder()
            .uuid("1").name("TestProduct").description("Test Description").price(1D).build();
    private static final Duration DEFAULT_VERIFY_TIMEOUT = Duration.ofSeconds(10);

    private ReactiveKafkaConsumerTemplate<String, ProductDto> reactiveKafkaConsumerTemplate;

    private ReactiveKafkaProducerTemplate<String, ProductDto> reactiveKafkaProducerTemplate;

    @Mock
    private ProductRepository repository;

    @Mock
    private ProductMapper mapper;

    private static ReceiverOptions<String, ProductDto> setupReceiverOptionsWithDefaultTopic(
            Map<String, Object> consumerProps) {

        ReceiverOptions<String, ProductDto> basicReceiverOptions = ReceiverOptions.create(consumerProps);
        return basicReceiverOptions
                .consumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .addAssignListener(p -> assertThat(p.iterator().next().topicPartition().topic())
                        .isEqualTo(TEST_TOPIC))
                .subscription(Collections.singletonList(TEST_TOPIC));
    }

    @BeforeEach
    public void setUp() {
        this.reactiveKafkaProducerTemplate = new ReactiveKafkaProducerTemplate<>(setupSenderOptionsWithDefaultTopic());
        Map<String, Object> consumerProps = KafkaTestUtils
                .consumerProps("kafka-service-consumer-group", "false", EmbeddedKafkaCondition.getBroker());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TYPE_MAPPINGS, "product:com.nordcloud.kafkaservice.model.dto.ProductDto");
        this.reactiveKafkaConsumerTemplate =
                new ReactiveKafkaConsumerTemplate<>(setupReceiverOptionsWithDefaultTopic(consumerProps));

    }

    @AfterEach
    public void tearDown() {
        this.reactiveKafkaProducerTemplate.close();
    }

    @Test
    public void givenSingleRecord_whenSendingIt_thenRecordShouldBeReceived() {
        // given when
        Mono<SenderResult<Void>> senderResultMono =
                this.reactiveKafkaProducerTemplate.send(TEST_TOPIC, DEFAULT_VALUE);

        // then
        StepVerifier.create(senderResultMono)
                .assertNext(senderResult -> {
                    assertThat(senderResult.recordMetadata())
                            .extracting(RecordMetadata::topic)
                            .isEqualTo(TEST_TOPIC);
                })
                .expectComplete()
                .verify(DEFAULT_VERIFY_TIMEOUT);

        StepVerifier.create(reactiveKafkaConsumerTemplate.receive().doOnNext(rr -> rr.receiverOffset().acknowledge()))
                .assertNext(receiverRecord -> assertThat(receiverRecord.value()).isEqualTo(DEFAULT_VALUE))
                .thenCancel()
                .verify(DEFAULT_VERIFY_TIMEOUT);
    }

    private SenderOptions<String, ProductDto> setupSenderOptionsWithDefaultTopic() {
        Map<String, Object> senderProps =
                KafkaTestUtils.producerProps(EmbeddedKafkaCondition.getBroker().getBrokersAsString());
        senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return SenderOptions.create(senderProps);
    }

}