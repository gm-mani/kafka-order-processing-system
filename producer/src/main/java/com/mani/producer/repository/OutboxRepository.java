package com.mani.producer.repository;

import com.mani.producer.entity.OutboxEvent;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {
    List<OutboxEvent> findByPublishedFalse();

    List<OutboxEvent> findByPublishedTrueAndCreatedAtBeforeOrderByCreatedAtAsc(
            LocalDateTime cutoff,
            Pageable pageable
    );
}
