package com.mani.consumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mani.consumer.model.OrderCreatedEvent;
import com.mani.consumer.repository.ProcessedOrderRepository;
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
public class KafkaConsumer {

    private final ProcessedOrderRepository repository;
    private final ObjectMapper objectMapper;

    public KafkaConsumer(ProcessedOrderRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    // NOTE on attempts = "3": this means 1 initial attempt + 2 retries (not 3 retries).
    // The value includes the original attempt. Retry topics created will be:
    //   orders-topic-retry-0  (after 1000ms)
    //   orders-topic-retry-1  (after 2000ms)
    //   orders-topic-dlt      (after all retries exhausted)
    @RetryableTopic(
            attempts = "3",
            backOff = @BackOff(delay = 1000, multiplier = 2),
            autoCreateTopics = "true"
    )
    @KafkaListener(topics = "orders-topic", groupId = "orders-group")
    public void consume(String message,
                        Acknowledgment acknowledgment,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) throws JsonProcessingException {

        log.info("RECEIVED MESSAGE => {}", message);

        OrderCreatedEvent order = objectMapper.readValue(message, OrderCreatedEvent.class);

        if (order.orderId().equals("poison")) {
            throw new RuntimeException("cannot process this order: " + order.orderId());
        }

        try {
            log.info("Instance received order [{}] from partition [{}] offset [{}]",
                    order.orderId(), partition, offset);

            repository.insertOrder(order.orderId());
            log.info("Order is marked processed: {}", order.orderId());

            acknowledgment.acknowledge();
            log.info("Order processed and offset committed: {}", order.orderId());

        } catch (DataIntegrityViolationException exception) {
            log.info("Duplicate — order already processed, skipping: {}", order.orderId());
            acknowledgment.acknowledge();
        }
    }

    @DltHandler
    public void handleDlt(String message,
                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                          @Header(KafkaHeaders.OFFSET) long offset) {
        log.error("DLT: message parked for investigation — partition [{}] offset [{}]", partition, offset);

        try {
            OrderCreatedEvent order = new ObjectMapper().readValue(message, OrderCreatedEvent.class);
            log.error("DLT: poison order [orderId={}, eventId={}, product={}] — requires manual review",
                    order.orderId(), order.eventId(), order.productName());
        } catch (JsonProcessingException e) {
            // Message wasn't even parseable — log raw payload so nothing is lost
            log.error("DLT: unparseable message payload: {}", message);
            log.error("DLT: parse error: {}", e.getMessage());
        }
    }
}
