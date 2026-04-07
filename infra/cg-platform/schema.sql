-- Common Ground Platform — Postgres schema
-- Subscriptions, change tracking, notification queue, delivery log
-- Database: cg (separate from ducklake catalog)

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- USERS — anonymous identity per channel
-- ============================================================

CREATE TABLE IF NOT EXISTS users (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    -- Channel-specific identity (one of these is set)
    telegram_id     BIGINT UNIQUE,
    email           TEXT UNIQUE,
    phone           TEXT UNIQUE,        -- E.164 format for SMS
    api_key_hash    TEXT UNIQUE,        -- SHA-256 of CLI API key
    -- Profile
    display_name    TEXT,
    language        TEXT DEFAULT 'en',  -- en, es, zh, ht (Haitian Creole)
    timezone        TEXT DEFAULT 'America/New_York',
    -- Lifecycle
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_active_at  TIMESTAMPTZ DEFAULT now(),
    suspended       BOOLEAN DEFAULT FALSE,
    suspended_reason TEXT
);

CREATE INDEX idx_users_telegram ON users(telegram_id) WHERE telegram_id IS NOT NULL;
CREATE INDEX idx_users_email ON users(email) WHERE email IS NOT NULL;
CREATE INDEX idx_users_phone ON users(phone) WHERE phone IS NOT NULL;

-- ============================================================
-- SUBSCRIPTIONS — what users want to watch
-- ============================================================

CREATE TABLE IF NOT EXISTS subscriptions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Subscription type
    sub_type        TEXT NOT NULL CHECK (sub_type IN (
        'address_watch',    -- BBL or address
        'entity_watch',     -- person/company name (with anti-stalking caps)
        'zip_category'      -- ZIP + table category
        -- NOTE: data_watch and keyword_watch deferred per security review
    )),
    -- The thing being watched
    filter_value    TEXT NOT NULL,           -- BBL, name, ZIP
    filter_extra    JSONB DEFAULT '{}',      -- categories, optional filters
    -- Display
    description     TEXT NOT NULL,           -- human-readable summary
    -- Delivery
    channels        TEXT[] NOT NULL,         -- ['telegram', 'email', 'sms']
    frequency       TEXT NOT NULL DEFAULT 'daily' CHECK (frequency IN ('realtime', 'daily', 'weekly')),
    -- Lifecycle
    active          BOOLEAN NOT NULL DEFAULT TRUE,
    paused_until    TIMESTAMPTZ,             -- snooze
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_notified_at TIMESTAMPTZ,
    notification_count INTEGER NOT NULL DEFAULT 0,
    -- Anti-stalking: cooling period for new entity_watch
    cooling_until   TIMESTAMPTZ
);

CREATE INDEX idx_subs_user ON subscriptions(user_id) WHERE active;
CREATE INDEX idx_subs_type_filter ON subscriptions(sub_type, filter_value) WHERE active;
CREATE INDEX idx_subs_channels ON subscriptions USING GIN(channels) WHERE active;

-- Anti-stalking caps enforced via trigger
CREATE OR REPLACE FUNCTION enforce_subscription_caps() RETURNS TRIGGER AS $$
DECLARE
    address_count INT;
    entity_count INT;
BEGIN
    IF NEW.sub_type = 'address_watch' THEN
        SELECT COUNT(*) INTO address_count
        FROM subscriptions
        WHERE user_id = NEW.user_id AND sub_type = 'address_watch' AND active;
        IF address_count >= 20 THEN
            RAISE EXCEPTION 'Maximum 20 address subscriptions per user';
        END IF;
    ELSIF NEW.sub_type = 'entity_watch' THEN
        SELECT COUNT(*) INTO entity_count
        FROM subscriptions
        WHERE user_id = NEW.user_id AND sub_type = 'entity_watch' AND active;
        IF entity_count >= 10 THEN
            RAISE EXCEPTION 'Maximum 10 entity subscriptions per user';
        END IF;
        -- Cooling period: 24h delay before first notification
        NEW.cooling_until := now() + INTERVAL '24 hours';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_subscription_caps
    BEFORE INSERT ON subscriptions
    FOR EACH ROW
    EXECUTE FUNCTION enforce_subscription_caps();

-- ============================================================
-- AUDIT LOG — every subscription action (anti-abuse)
-- ============================================================

CREATE TABLE IF NOT EXISTS audit_log (
    id              BIGSERIAL PRIMARY KEY,
    user_id         UUID REFERENCES users(id),
    action          TEXT NOT NULL,           -- 'create', 'update', 'delete', 'pause', 'notify'
    sub_id          UUID,
    sub_type        TEXT,
    filter_value    TEXT,
    metadata        JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_user_time ON audit_log(user_id, created_at DESC);
CREATE INDEX idx_audit_filter ON audit_log(filter_value) WHERE action = 'create';

-- ============================================================
-- SNAPSHOT BOOKMARKS — DuckLake CDC tracking
-- ============================================================

CREATE TABLE IF NOT EXISTS snapshot_bookmarks (
    table_key       TEXT PRIMARY KEY,        -- 'housing.hpd_violations'
    last_snapshot   BIGINT NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- NOTIFICATION QUEUE — pending deliveries
-- ============================================================

CREATE TABLE IF NOT EXISTS notification_queue (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id),
    sub_id          UUID NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    channel         TEXT NOT NULL,           -- 'telegram', 'email', 'sms'
    -- Payload
    title           TEXT NOT NULL,
    summary         TEXT NOT NULL,
    payload         JSONB NOT NULL,          -- full structured data for rendering
    -- Scheduling
    frequency       TEXT NOT NULL,           -- copied from subscription at queue time
    scheduled_for   TIMESTAMPTZ NOT NULL DEFAULT now(),  -- when to send (digest time for daily)
    -- Status
    status          TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed', 'skipped')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    sent_at         TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_queue_pending ON notification_queue(scheduled_for, status) WHERE status = 'pending';
CREATE INDEX idx_queue_user ON notification_queue(user_id, created_at DESC);

-- ============================================================
-- DELIVERY LOG — what actually got sent (rate limiting + retry)
-- ============================================================

CREATE TABLE IF NOT EXISTS delivery_log (
    id              BIGSERIAL PRIMARY KEY,
    queue_id        UUID NOT NULL REFERENCES notification_queue(id),
    user_id         UUID NOT NULL REFERENCES users(id),
    channel         TEXT NOT NULL,
    status          TEXT NOT NULL,           -- 'delivered', 'failed', 'bounced'
    response_code   INTEGER,
    response_body   TEXT,
    delivered_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_delivery_user_time ON delivery_log(user_id, delivered_at DESC);
CREATE INDEX idx_delivery_rate_limit ON delivery_log(user_id, channel, delivered_at);

-- ============================================================
-- API KEYS — for CLI authentication
-- ============================================================

CREATE TABLE IF NOT EXISTS api_keys (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    key_hash        TEXT NOT NULL UNIQUE,    -- SHA-256 of the actual key
    key_prefix      TEXT NOT NULL,           -- first 8 chars for display: "cg_live_abc12..."
    name            TEXT,                    -- user-given label
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at    TIMESTAMPTZ,
    revoked_at      TIMESTAMPTZ
);

CREATE INDEX idx_apikeys_hash ON api_keys(key_hash) WHERE revoked_at IS NULL;
