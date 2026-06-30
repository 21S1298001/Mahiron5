CREATE TABLE common_data_announcements (
    original_network_id INTEGER NOT NULL,
    transport_stream_id INTEGER NOT NULL,
    service_id INTEGER NOT NULL,
    download_id INTEGER NOT NULL,
    version_id INTEGER NOT NULL,
    observed_channel_type TEXT NOT NULL,
    observed_channel_id TEXT NOT NULL,
    seen_at INTEGER NOT NULL,
    PRIMARY KEY (original_network_id, transport_stream_id, service_id)
);
