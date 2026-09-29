package com.mani.producer.service;


import com.mani.producer.entity.OutboxEvent;
import com.mani.producer.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
public class OutboxCleanupService {
    private final OutboxRepository outboxRepository;

    public void cleanup(int retentionDays, int batchSize) {

        LocalDateTime cutoff = LocalDateTime.now().minusDays(retentionDays);

        Pageable pageable = PageRequest.of(
                0,
                batchSize,
                Sort.by("createdAt").ascending()
        );

        List<OutboxEvent> events =
                outboxRepository.findByPublishedTrueAndCreatedAtBeforeOrderByCreatedAtAsc(
                        cutoff, pageable
                );

        if (!events.isEmpty()) {
            outboxRepository.deleteAllInBatch(events);
        }
    }
}
