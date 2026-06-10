package com.mani.producer.OutboxPublisher;

import com.mani.producer.Entity.OutboxEvent;
import com.mani.producer.Repository.OutboxRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@AllArgsConstructor
@Slf4j
public class OutboxPublisher {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 5000)
    public void publishEvents() {

        List<OutboxEvent> events =
                outboxRepository.findByPublishedFalse();

        if (events.isEmpty()) {
            log.debug("No unpublished events found");
            return;
        }

        log.info("Publishing {} unpublished event(s)", events.size());

        for (OutboxEvent event : events) {

            kafkaTemplate.send(
                    "orders-topic",
                    event.getAggregateId(),
                    event.getPayload()
            );

            log.debug("Event published to Kafka - Event ID: {}, Order ID: {}", event.getEventId(), event.getAggregateId());

            event.setPublished(true);

            outboxRepository.save(event);
        }

        log.info("All {} event(s) marked as published", events.size());
    }
}
