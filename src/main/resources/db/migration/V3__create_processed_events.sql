CREATE TABLE processed_events (
    event_id     UUID      NOT NULL,
    processed_at TIMESTAMP NOT NULL,
    CONSTRAINT pk_processed_events PRIMARY KEY (event_id)
);
