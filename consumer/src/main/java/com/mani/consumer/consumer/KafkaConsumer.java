package com.mani.consumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mani.consumer.repository.ProcessedOrderRepository;
import com.mani.kafka.avro.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumer {

    private final ProcessedOrderRepository repository;

    // NOTE on attempts = "3": this means 1 initial attempt + 2 retries (not 3 retries).
    @RetryableTopic(
            attempts = "3",
            backOff = @BackOff(delay = 1000, multiplier = 2),
            autoCreateTopics = "true"
    )
    @KafkaListener(topics = "orders-topic", groupId = "orders-group")
    public void consume(OrderCreatedEvent order,
                        Acknowledgment acknowledgment,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {

        if (order.getOrderId().equals("poison")) {
            throw new RuntimeException("cannot process this order: " + order.getOrderId());
        }

        try {
            log.info("Instance received order [{}] from partition [{}] offset [{}]",
                    order.getOrderId(), partition, offset);

            repository.insertOrder(order.getOrderId().toString());
            log.info("Order is marked processed: {}", order.getOrderId());

            acknowledgment.acknowledge();
            log.info("Order processed and offset committed: {}", order.getOrderId());

        } catch (DataIntegrityViolationException exception) {
            log.info("Duplicate — order already processed, skipping: {}", order.getOrderId());
            acknowledgment.acknowledge();
        }
    }

    @DltHandler
    public void handleDlt(OrderCreatedEvent order,
                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                          @Header(KafkaHeaders.OFFSET) long offset) {

        log.error("DLT: message parked for investigation — partition [{}] offset [{}]", partition, offset);
        log.error("DLT: poison order [orderId={}, eventId={}, product={}] — requires manual review",
                order.getOrderId(), order.getEventId(), order.getProductName());

    }
}
