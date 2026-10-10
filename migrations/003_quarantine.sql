-- Migration 003 — quarantine for readings stamped too far in the future.
--
-- "Newest event_time wins" means one reading from a broken clock (stamped
-- 2099, say) would become current state forever. Such a reading is kept in
-- history exactly as sent (never lost, so a human can correct it later) but
-- flagged and never allowed to drive current state.
--
-- The verdict is made ONCE, against the time the API received the reading,
-- and stored here. Re-judging later against now() would let a reading stamped
-- 10 minutes ahead quietly become legitimate 10 minutes later.

ALTER TABLE readings ADD COLUMN IF NOT EXISTS received_at TIMESTAMPTZ;
ALTER TABLE readings ADD COLUMN IF NOT EXISTS quarantined BOOLEAN NOT NULL DEFAULT false;

-- Gateway-level flag for the dashboard: set by a quarantined reading, cleared
-- by the gateway's next reading with a sane timestamp. No background job.
ALTER TABLE gateway_state ADD COLUMN IF NOT EXISTS clock_ahead BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE gateway_state ADD COLUMN IF NOT EXISTS last_clock_ahead_at TIMESTAMPTZ;
