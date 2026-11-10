-- Transactional outbox: dispatch messages are written in the SAME transaction that
-- creates the run, then relayed to Kafka. Avoids the dual-write problem
-- (DB commit succeeds, Kafka publish fails → run stuck QUEUED forever).
CREATE TABLE outbox (
    id            UUID PRIMARY KEY,
    topic         VARCHAR(200) NOT NULL,
    message_key   VARCHAR(200) NOT NULL,
    payload       TEXT         NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ,
    attempts      INT          NOT NULL DEFAULT 0
);

CREATE INDEX idx_outbox_unpublished ON outbox (created_at) WHERE published_at IS NULL;

-- Which handler a worker should use for this job (e.g. "log", "http", "sleep").
ALTER TABLE jobs ADD COLUMN handler VARCHAR(100) NOT NULL DEFAULT 'log';
