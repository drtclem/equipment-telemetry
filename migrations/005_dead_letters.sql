-- Migration 005 — dead letters.
--
-- A queue entry that keeps failing to apply must neither be dropped (that
-- would lose a reading) nor retried forever (one bad entry crash-looped the
-- worker and stalled ingestion for every gateway). After WORKER_MAX_ATTEMPTS
-- failed deliveries it is parked here with its raw payload exactly as queued
-- and the error, then acknowledged. It can be inspected and replayed once the
-- cause is fixed. Unique on the stream entry id so a retry logs it once.

CREATE TABLE IF NOT EXISTS dead_letters (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream_entry_id TEXT NOT NULL UNIQUE,
    fields          JSONB NOT NULL,
    error           TEXT NOT NULL,
    attempts        INTEGER NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
