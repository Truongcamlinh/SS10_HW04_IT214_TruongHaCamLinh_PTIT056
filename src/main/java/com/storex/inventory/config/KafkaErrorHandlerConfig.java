package com.storex.inventory.config;

import com.storex.inventory.model.OrderEvent;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlerConfig.class);
    private static final long RETRY_INTERVAL_MS = 1000L;
    private static final long MAX_RETRY_ATTEMPTS = 3L;

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaOperations<Object, Object> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, exception) -> {
            log.error("Send message to DLQ: topic={}, partition={}, offset={}, reason={}",
                    record.topic(), record.partition(), record.offset(), exception.getMessage());
            return new TopicPartition(record.topic() + ".DLQ", record.partition());
        });
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRY_ATTEMPTS));

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Retry order event: attempt={}, topic={}, partition={}, offset={}, error={}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage()));

        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }
}

