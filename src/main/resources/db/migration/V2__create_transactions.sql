CREATE TABLE transactions (
    id             UUID          NOT NULL,
    account_id     UUID          NOT NULL,
    type           VARCHAR(20)   NOT NULL,
    amount         NUMERIC(19,4) NOT NULL,
    balance_after  NUMERIC(19,4) NOT NULL,
    correlation_id UUID          NOT NULL,
    occurred_at    TIMESTAMP     NOT NULL,
    CONSTRAINT pk_transactions PRIMARY KEY (id),
    CONSTRAINT fk_transactions_account FOREIGN KEY (account_id) REFERENCES accounts (id)
);

CREATE INDEX idx_transactions_account_id ON transactions (account_id);
