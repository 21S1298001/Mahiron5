CREATE TABLE IF NOT EXISTS service_logos_new (
    network_id INTEGER NOT NULL,
    transport_stream_id INTEGER NOT NULL,
    service_id INTEGER NOT NULL,
    logo_id INTEGER NOT NULL,
    logo_type INTEGER NOT NULL,
    logo_version INTEGER NOT NULL,
    download_data_id INTEGER NOT NULL,
    data BLOB NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (network_id, transport_stream_id, service_id, logo_id, logo_type)
);

INSERT OR REPLACE INTO service_logos_new (
    network_id,
    transport_stream_id,
    service_id,
    logo_id,
    logo_type,
    logo_version,
    download_data_id,
    data,
    updated_at
)
SELECT
    l.network_id,
    COALESCE(s.transport_stream_id, 0),
    l.service_id,
    l.logo_id,
    l.logo_type,
    l.logo_version,
    l.download_data_id,
    l.data,
    l.updated_at
FROM service_logos l
LEFT JOIN services s
  ON s.network_id = l.network_id
 AND s.service_id = l.service_id;

DROP TABLE service_logos;
ALTER TABLE service_logos_new RENAME TO service_logos;
