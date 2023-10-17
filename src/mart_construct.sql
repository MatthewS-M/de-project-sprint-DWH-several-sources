create table if not exists cdm.dm_courier_ledger (
    id SERIAL PRIMARY KEY,
    courier_id text, 
    courier_name VARCHAR(255),
    settlement_year INT,
    settlement_month INT check (settlement_month>=1 and settlement_month<=12),
    orders_count INT,
    orders_total_sum numeric(14,2),
    rate_avg decimal(4, 2),
    order_processing_fee numeric(14,2),
    courier_order_sum numeric(14,2),
    courier_tips_sum numeric(14,2),
    courier_reward_sum numeric(14,2),
    CONSTRAINT courier_id_year_month_unique unique(courier_id, settlement_year, settlement_year)
);
