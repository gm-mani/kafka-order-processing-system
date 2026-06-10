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

            OrderCreatedEvent order =
                    objectMapper.readValue(
                            message,
                            OrderCreatedEvent.class);

            // simulate retry
            if (order.orderId().equals("poison")) {
                throw new RuntimeException("cannot process this order: " + order.orderId());
            }

            try {

                log.info("Instance received order [{}] from partition [{}] offset [{}]",
                        order.orderId(), partition, offset);

                // simulate db and handle idempotency
                repository.insertOrder(order.orderId());
                log.info("Order is marked processed: {}", order.orderId());

                // Manual offset commit
                acknowledgment.acknowledge();
                log.info("Order processed: {}", order.orderId());
            } catch (DataIntegrityViolationException exception) {
                log.info("Order already processed: {}", order.orderId());
                return;
            }

        }

        @DltHandler
        public void handleDlt(String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition, @Header(KafkaHeaders.OFFSET) long offset) throws JsonProcessingException {

            log.error("DLT RAW MESSAGE => {}", message);
//            OrderCreatedEvent order =
//                    objectMapper.readValue(
//                            message,
//                            OrderCreatedEvent.class);
//            log.error("DLT received poison order [{}] from partition [{}] offset [{}] — parked for investigation",
//                    order.orderId(), partition, offset);
        }

    }
