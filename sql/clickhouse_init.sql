CREATE DATABASE IF NOT EXISTS stg;
CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS dm;


CREATE TABLE IF NOT EXISTS stg.coin_history
(
    date Date,
    coin_id String,
    price_usd Nullable(Float64),
    market_cap_usd Nullable(Float64),
    total_volume_usd Nullable(Float64)
)
ENGINE = MergeTree()
PRIMARY KEY (coin_id, date)
ORDER BY (coin_id, date);