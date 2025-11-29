CREATE TABLE IF NOT EXISTS app_user_visits_fact
(
    id String,
    phone_number Nullable(String),
    customer_id String,
    branch_id String,
    store_id String,
    cashier_id String,
    countryCode Nullable(String),
    seen Nullable(Int32),
    state Nullable(Int32),
    expired Nullable(Int32),
    points Nullable(Float64),
    receipt Nullable(Float64),
    remaining Nullable(Float64),
    created_at Nullable(Int64),
    updated_at Nullable(Int64),
    expires_at Nullable(Int64),
    order_id Nullable(String),
    is_deleted Int16 DEFAULT 0,
    is_fraud Int16 DEFAULT 0,
    sync_mechanism Nullable(String),
    is_bulk_points Nullable(String)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(toDateTime(created_at / 1000))
ORDER BY (store_id, branch_id, customer_id, id)
SETTINGS index_granularity = 8192;