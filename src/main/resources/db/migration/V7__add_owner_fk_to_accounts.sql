ALTER TABLE accounts
    ADD CONSTRAINT fk_accounts_owner FOREIGN KEY (owner_id) REFERENCES users (id);
