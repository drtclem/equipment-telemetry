-- Migration 004 — freshness and the gateway outage log.
--
-- Late-data policy: the live view is never held back (commissioning tests
-- need seconds-level feedback; newest-event-time-wins already makes arrival
-- order irrelevant for current state). Instead the system says when what it
-- shows may be behind, and why:
--   * gateway silent  — no reading received from it for SILENCE_THRESHOLD
--   * device quiet    — its gateway is reporting, but this metric isn't
-- Silence is measured on the API's receive clock (received_at), so a backed-up
-- queue or a stopped worker never looks like a gateway outage.

ALTER TABLE gateway_state ADD COLUMN IF NOT EXISTS last_received_at TIMESTAMPTZ;

-- One row per outage, written when a silent gateway speaks again. A report
-- that depends on a time window can use this to know whether that window may
-- have been filled in after the fact. Unique so a redelivered entry logs once.
CREATE TABLE IF NOT EXISTS gateway_outages (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    gateway_id   TEXT NOT NULL,
    silent_since TIMESTAMPTZ NOT NULL,   -- last reading received before the gap
    resumed_at   TIMESTAMPTZ NOT NULL,   -- first reading received after it
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (gateway_id, silent_since)
);

CREATE INDEX IF NOT EXISTS gateway_outages_resumed_idx ON gateway_outages (resumed_at DESC);

-- Replaced by the above: the "reconciling" badge only appeared after a gateway
-- reconnected (after the risky period) and could stay on forever.
ALTER TABLE gateway_state DROP COLUMN IF EXISTS replaying;
ALTER TABLE gateway_state DROP COLUMN IF EXISTS last_late_at;
