-- 01-schema.sql
-- Initial schema migration applied across all PostgreSQL shards on container startup.

-- Enable UUID extension for auto-generating UUIDv4/v7 identifiers
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ENUMs
CREATE TYPE transaction_type AS ENUM (
    'expense',
    'transfer',
    'reimbursement',
    'income'
);

-- 1. USERS TABLE
-- Target Shard Key: user_id (Self/Primary Key)
CREATE TABLE IF NOT EXISTS users (
    id          UUID PRIMARY KEY DEFAULT uuidv7(),
    username    VARCHAR(255) NOT NULL UNIQUE,
    password    VARCHAR(255) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at  TIMESTAMPTZ
);

-- 2. FOREX RATES TABLE (Global Reference Table)
-- Strategy: Replicated in FULL across ALL shards so local joins can resolve FX.
CREATE TABLE IF NOT EXISTS forex_rates (
    id             UUID PRIMARY KEY DEFAULT uuidv7(),
    base_currency  VARCHAR(10) NOT NULL,
    quote_currency VARCHAR(10) NOT NULL,
    exchange_rate  NUMERIC(18, 6) NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at     TIMESTAMPTZ,
    CONSTRAINT uq_forex_pair UNIQUE (base_currency, quote_currency)
);

-- 3. ACCOUNTS TABLE
-- Target Shard Key: user_id
CREATE TABLE IF NOT EXISTS accounts (
    id               UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id          UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name             VARCHAR(255) NOT NULL,
    type             VARCHAR(50) NOT NULL,
    currency         VARCHAR(10) NOT NULL,
    exchange_rate_id UUID REFERENCES forex_rates(id) ON DELETE SET NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_accounts_user_id ON accounts(user_id);

-- 4. CATEGORIES TABLE
-- Target Shard Key: user_id

CREATE TABLE IF NOT EXISTS categories (
    id         UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name       VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_categories_user_id ON categories(user_id);

-- 5. TRANSACTIONS TABLE
-- Target Shard Key: user_id

CREATE TABLE IF NOT EXISTS transactions (
    id              UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    to_account_id   UUID REFERENCES accounts(id) ON DELETE SET NULL,
    from_account_id UUID REFERENCES accounts(id) ON DELETE SET NULL,
    type            transaction_type NOT NULL,
    date            TIMESTAMPTZ NOT NULL,
    description     TEXT NOT NULL DEFAULT '',
    settles         UUID[] NOT NULL DEFAULT '{}',
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date);

-- 6. TRANSACTION LINE ITEMS TABLE
-- Target Shard Key: user_id
-- Co-located with parent transaction and user on the same shard.

CREATE TABLE IF NOT EXISTS transaction_line_items (
    id             UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    category_id    UUID REFERENCES categories(id) ON DELETE SET NULL,
    amount         NUMERIC(18, 4) NOT NULL, -- Numeric is better than float because better precision
    currency       VARCHAR(10) NOT NULL,
    exchange_rate  NUMERIC(18, 6) NOT NULL DEFAULT 1.0,
    description    TEXT NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_line_items_user_id ON transaction_line_items(user_id);
CREATE INDEX IF NOT EXISTS idx_line_items_transaction_id ON transaction_line_items(transaction_id);
