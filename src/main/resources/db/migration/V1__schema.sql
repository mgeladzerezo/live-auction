-- All timestamps are written from the database clock (clock_timestamp()), never from a JVM.
-- All money columns are integer minor units (cents).

CREATE TABLE users (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username      TEXT        NOT NULL,
    password_hash TEXT        NOT NULL,
    bot           BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT users_username_format CHECK (username ~ '^[A-Za-z0-9_.-]{3,32}$')
);
CREATE UNIQUE INDEX users_username_key ON users (lower(username));

-- Opaque bearer tokens. Only the SHA-256 of a token is stored, so a database dump is not a
-- set of usable credentials.
CREATE TABLE auth_tokens (
    token_hash TEXT        PRIMARY KEY,
    user_id    BIGINT      NOT NULL REFERENCES users (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE auctions (
    id                        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    title                     TEXT        NOT NULL,
    description               TEXT        NOT NULL DEFAULT '',
    seller_id                 BIGINT      REFERENCES users (id),
    demo_key                  TEXT,
    start_price               BIGINT      NOT NULL CHECK (start_price > 0),
    min_increment             BIGINT      NOT NULL CHECK (min_increment > 0),
    reserve_price             BIGINT      CHECK (reserve_price IS NULL OR reserve_price > 0),
    starts_at                 TIMESTAMPTZ NOT NULL,
    ends_at                   TIMESTAMPTZ NOT NULL,
    original_ends_at          TIMESTAMPTZ NOT NULL,
    anti_snipe_window_seconds INT         NOT NULL CHECK (anti_snipe_window_seconds >= 0),
    max_extensions            INT         NOT NULL CHECK (max_extensions >= 0),
    extension_count           INT         NOT NULL DEFAULT 0,
    status                    TEXT        NOT NULL DEFAULT 'SCHEDULED'
        CHECK (status IN ('SCHEDULED', 'OPEN', 'CLOSED', 'SETTLED', 'UNSOLD')),
    -- Denormalised head of the bid history. Derivable from bids; the audit proves they agree.
    current_price             BIGINT,
    leader_id                 BIGINT      REFERENCES users (id),
    bid_count                 INT         NOT NULL DEFAULT 0,
    -- Optimistic-lock token: every change to an auction row increments it.
    version                   BIGINT      NOT NULL DEFAULT 0,
    -- Sequence number of the last event published for this auction.
    last_seq                  BIGINT      NOT NULL DEFAULT 0,
    closed_at                 TIMESTAMPTZ,
    settle_error              TEXT,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT auctions_time_order CHECK (ends_at > starts_at AND ends_at >= original_ends_at),
    CONSTRAINT auctions_extension_bound CHECK (extension_count BETWEEN 0 AND max_extensions),
    CONSTRAINT auctions_head_consistent CHECK ((current_price IS NULL) = (leader_id IS NULL)
        AND (current_price IS NULL) = (bid_count = 0))
);
-- Partial indexes that keep the scheduler's "what is due?" scans tiny regardless of history.
CREATE INDEX auctions_due_to_open ON auctions (starts_at) WHERE status = 'SCHEDULED';
CREATE INDEX auctions_due_to_close ON auctions (ends_at) WHERE status = 'OPEN';
CREATE INDEX auctions_to_settle ON auctions (id) WHERE status = 'CLOSED' AND settle_error IS NULL;
CREATE INDEX auctions_demo_key ON auctions (demo_key, id DESC) WHERE demo_key IS NOT NULL;

-- One row per bid attempt that received a definitive answer, keyed by the id the client chose.
-- The primary key is what makes a retried bid idempotent.
CREATE TABLE bid_attempts (
    bidder_id     BIGINT      NOT NULL REFERENCES users (id),
    client_bid_id TEXT        NOT NULL CHECK (client_bid_id ~ '^[A-Za-z0-9_-]{1,64}$'),
    auction_id    BIGINT      NOT NULL,
    amount        BIGINT      NOT NULL,
    outcome       TEXT        NOT NULL CHECK (outcome IN ('ACCEPTED', 'REJECTED')),
    reason        TEXT,
    decided_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (bidder_id, client_bid_id),
    CONSTRAINT bid_attempts_reason CHECK ((outcome = 'REJECTED') = (reason IS NOT NULL))
);
CREATE INDEX bid_attempts_auction ON bid_attempts (auction_id);

-- Accepted bids only. Append-only (see trigger below).
CREATE TABLE bids (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    auction_id    BIGINT      NOT NULL REFERENCES auctions (id),
    bidder_id     BIGINT      NOT NULL,
    client_bid_id TEXT        NOT NULL,
    amount        BIGINT      NOT NULL CHECK (amount > 0),
    -- Sequence number of the BID_ACCEPTED event; gives bids a total order per auction.
    seq           BIGINT      NOT NULL,
    -- The database clock reading the acceptance decision was made against.
    accepted_at   TIMESTAMPTZ NOT NULL,
    -- New end time if this bid triggered an anti-sniping extension.
    extended_to   TIMESTAMPTZ,
    CONSTRAINT bids_attempt_fk FOREIGN KEY (bidder_id, client_bid_id)
        REFERENCES bid_attempts (bidder_id, client_bid_id),
    CONSTRAINT bids_one_per_attempt UNIQUE (bidder_id, client_bid_id),
    CONSTRAINT bids_seq_unique UNIQUE (auction_id, seq),
    -- Prices strictly increase, so no two accepted bids on an auction can share an amount.
    -- A second line of defence under the locking logic.
    CONSTRAINT bids_amount_unique UNIQUE (auction_id, amount)
);

-- The ordered event log every client stream is built from.
CREATE TABLE auction_events (
    auction_id BIGINT      NOT NULL REFERENCES auctions (id),
    seq        BIGINT      NOT NULL CHECK (seq > 0),
    type       TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (auction_id, seq)
);

CREATE FUNCTION forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: % is not allowed', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$;

CREATE TRIGGER bids_append_only BEFORE UPDATE OR DELETE ON bids
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER bid_attempts_append_only BEFORE UPDATE OR DELETE ON bid_attempts
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER auction_events_append_only BEFORE UPDATE OR DELETE ON auction_events
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
