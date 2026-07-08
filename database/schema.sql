-- ==========================================
-- 1. جدول الحالة المركزية (System State)
-- يحتوي على سطر واحد فقط (id = 1) لمنع التضارب
-- ==========================================
CREATE TABLE IF NOT EXISTS system_state (
    id INT PRIMARY KEY DEFAULT 1,
    active_worker VARCHAR(50) DEFAULT 'none',
    worker_start_time TIMESTAMPTZ,
    last_worker_heartbeat TIMESTAMPTZ,
    active_watchdog VARCHAR(50) DEFAULT 'none',
    watchdog_start_time TIMESTAMPTZ,
    last_watchdog_heartbeat TIMESTAMPTZ,
    backup_attempts INT DEFAULT 0,
    CONSTRAINT single_row_constraint CHECK (id = 1)
);

-- إدراج السطر الأساسي إذا لم يكن موجوداً
INSERT INTO system_state (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- ==========================================
-- 2. جدول أثر الجريمة (Worker Heartbeats)
-- يحتفظ بسجل لكل عامل لتشخيص الأعطال
-- ==========================================
CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_name VARCHAR(50) PRIMARY KEY,
    last_heartbeat TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT FALSE
);

-- ==========================================
-- 3. جدول الصندوق الأسود (Event Log)
-- سجل تراكمي للأحداث لا يتم تعديله أو حذفه
-- ==========================================
CREATE TABLE IF NOT EXISTS event_log (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    component VARCHAR(50) NOT NULL,
    name VARCHAR(50) NOT NULL,
    message TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- إنشاء الفهارس لتسريع البحث
CREATE INDEX IF NOT EXISTS idx_event_log_created_at ON event_log(created_at);
CREATE INDEX IF NOT EXISTS idx_worker_heartbeats_name ON worker_heartbeats(worker_name);