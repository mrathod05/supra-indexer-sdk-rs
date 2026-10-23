CREATE TABLE tokens_created (
    token_address    TEXT PRIMARY KEY,
    creator          TEXT NOT NULL,
    initial_supply   NUMERIC(39, 0) NOT NULL,
    created_at_secs  BIGINT NOT NULL,
    block_height     BIGINT NOT NULL,
    transaction_hash TEXT
);

CREATE INDEX idx_tokens_created_creator ON tokens_created (creator);
