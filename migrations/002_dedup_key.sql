-- Migration 002 — reading identity and the conflicts ledger.
--
-- A reading's identity is who measured what, and when:
--   (gateway_id, device_id, metric, sequence, event_time)
-- The brief never promises `sequence` is unique (the provided generator numbers
-- it differently per scenario, and a rebooted gateway restarts its counter), so
-- sequence alone cannot tell two readings apart. A real resend is byte-identical
-- and still collapses to one row; a reused counter with a new timestamp is a new
-- reading and is kept.

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conname = 'readings_gateway_id_device_id_metric_sequence_key') THEN
        ALTER TABLE readings DROP CONSTRAINT readings_gateway_id_device_id_metric_sequence_key;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'readings_identity_key') THEN
        ALTER TABLE readings ADD CONSTRAINT readings_identity_key
            UNIQUE (gateway_id, device_id, metric, sequence, event_time);
    END IF;
END $$;

-- Same identity, different value: not a resend, something is wrong upstream.
-- Recorded here instead of silently dropped, so every delivery ends up either
-- in history or in this ledger. Unique so a redelivered queue entry logs once.
CREATE TABLE IF NOT EXISTS reading_conflicts (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    gateway_id     TEXT NOT NULL,
    device_id      TEXT NOT NULL,
    metric         TEXT NOT NULL,
    sequence       BIGINT NOT NULL,
    event_time     TIMESTAMPTZ NOT NULL,
    stored_value   DOUBLE PRECISION NOT NULL,
    incoming_value DOUBLE PRECISION NOT NULL,
    detected_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (gateway_id, device_id, metric, sequence, event_time, incoming_value)
);
