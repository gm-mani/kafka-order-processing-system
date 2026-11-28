package com.mani.producer.repository;

import com.mani.producer.entity.OutboxEvent;
import com.mani.producer.entity.OutboxStatus;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    // Must run inside a transaction: the row locks are held until it commits
    @Query(value = """
            SELECT * FROM outbox_events
            WHERE status = 'PENDING'
            ORDER BY id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED 
            """, nativeQuery = true)
    List<OutboxEvent> lockNextBatch(@Param("batchSize") int batchSize);

    List<OutboxEvent> findByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
            OutboxStatus status, LocalDateTime cutoff, Pageable pageable);

}
