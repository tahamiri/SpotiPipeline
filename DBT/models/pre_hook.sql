CREATE TABLE IF NOT EXISTS auth_events
USING PARQUET
LOCATION 'hdfs://localhost:8020/user/bronze/auth_events';

CREATE TABLE IF NOT EXISTS listen_events
USING PARQUET
LOCATION 'hdfs://localhost:8020/user/bronze/listen_events';

CREATE TABLE IF NOT EXISTS page_view_events
USING PARQUET
LOCATION 'hdfs://localhost:8020/user/bronze/page_view_events';

CREATE TABLE IF NOT EXISTS status_change_events
USING PARQUET
LOCATION 'hdfs://localhost:8020/user/bronze/status_change_events';