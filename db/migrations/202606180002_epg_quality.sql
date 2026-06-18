ALTER TABLE programs ADD COLUMN extended TEXT;
ALTER TABLE programs ADD COLUMN related_items TEXT;
ALTER TABLE programs ADD COLUMN series TEXT;

CREATE TABLE IF NOT EXISTS epg_service_status (
    network_id INTEGER NOT NULL,
    service_id INTEGER NOT NULL,
    last_attempt_at INTEGER,
    last_success_at INTEGER,
    last_error TEXT,
    PRIMARY KEY (network_id, service_id)
);
