-- ═══════════════════════════════════════════════
-- 24/7 Monitoring Bot — Database Schema
-- ═══════════════════════════════════════════════

-- ─── system_state: صف واحد فقط (Singleton) ───
CREATE TABLE IF NOT EXISTS system_state (
    id                INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    active_worker     TEXT        DEFAULT 'none',
    worker_start_time TIMESTAMPTZ,
    status            TEXT        DEFAULT 'IDLE',
    updated_at        TIMESTAMPTZ DEFAULT now()
);

-- ─── worker_heartbeats ───
CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_name TEXT PRIMARY KEY,
    status      TEXT,
    last_beat   TIMESTAMPTZ DEFAULT now(),
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- ─── watchdog_heartbeats ───
CREATE TABLE IF NOT EXISTS watchdog_heartbeats (
    watchdog_name TEXT PRIMARY KEY,
    status        TEXT,
    last_beat     TIMESTAMPTZ DEFAULT now(),
    created_at    TIMESTAMPTZ DEFAULT now()
);

-- ─── event_log ───
CREATE TABLE IF NOT EXISTS event_log (
    id         SERIAL PRIMARY KEY,
    event_type TEXT,
    severity   TEXT,
    details    JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- ─── Indexes ───
CREATE INDEX IF NOT EXISTS idx_event_log_created_at ON event_log (created_at);
CREATE INDEX IF NOT EXISTS idx_event_log_severity   ON event_log (severity);
CREATE INDEX IF NOT EXISTS idx_worker_hb_time        ON worker_heartbeats (last_beat);

-- ─── Row Level Security ───
ALTER TABLE system_state         ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_heartbeats    ENABLE ROW LEVEL SECURITY;
ALTER TABLE watchdog_heartbeats  ENABLE ROW LEVEL SECURITY;
ALTER TABLE event_log            ENABLE ROW LEVEL SECURITY;

-- ─── Policies: anon can only SELECT from system_state & event_log ───
DROP POLICY IF EXISTS "anon_select_system_state" ON system_state;
CREATE POLICY "anon_select_system_state"
    ON system_state FOR SELECT TO anon USING (true);

DROP POLICY IF EXISTS "anon_select_event_log" ON event_log;
CREATE POLICY "anon_select_event_log"
    ON event_log FOR SELECT TO anon USING (true);

-- ─── Initial row for system_state ───
INSERT INTO system_state (id, active_worker, status)
VALUES (1, 'none', 'IDLE')
ON CONFLICT DO NOTHING;