CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS services (
    id TEXT PRIMARY KEY,
    service_id INTEGER NOT NULL,
    network_id INTEGER NOT NULL,
    transport_stream_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    type INTEGER NOT NULL,
    remote_control_key_id INTEGER NOT NULL,
    channel_type TEXT NOT NULL,
    channel_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_services_channel ON services(channel_type, channel_id);

CREATE TABLE IF NOT EXISTS programs (
    id INTEGER PRIMARY KEY,
    event_id INTEGER NOT NULL,
    service_id INTEGER NOT NULL,
    network_id INTEGER NOT NULL,
    start_at INTEGER NOT NULL,
    duration INTEGER NOT NULL,
    is_free INTEGER NOT NULL,
    name TEXT,
    description TEXT,
    genres TEXT,
    video TEXT,
    audios TEXT
);

CREATE INDEX IF NOT EXISTS idx_programs_service ON programs(network_id, service_id);
CREATE INDEX IF NOT EXISTS idx_programs_start_at ON programs(start_at);
CREATE INDEX IF NOT EXISTS idx_programs_ended_at ON programs(start_at + duration);
