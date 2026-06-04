CREATE TABLE accounts (
    id          UUID          NOT NULL,
    owner_id    UUID          NOT NULL,
    currency    VARCHAR(3)    NOT NULL,
    balance     NUMERIC(19,4) NOT NULL DEFAULT 0,
    created_at  TIMESTAMP     NOT NULL,
    updated_at  TIMESTAMP     NOT NULL,
    CONSTRAINT pk_accounts PRIMARY KEY (id)
);
