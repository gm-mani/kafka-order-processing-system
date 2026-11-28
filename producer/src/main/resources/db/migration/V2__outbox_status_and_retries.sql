ALTER TABLE outbox_events ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'PENDING';
ALTER TABLE outbox_events ADD COLUMN retry_count INT NOT NULL DEFAULT 0;
ALTER TABLE outbox_events ADD COLUMN last_error TEXT;

UPDATE outbox_events SET status = 'PUBLISHED' WHERE published = TRUE;

ALTER TABLE outbox_events DROP COLUMN published;

CREATE INDEX idx_outbox_status_id ON outbox_events (status, id);

