with courier_order_sum as (
    select del.courier_id, case when avg(del.rate)<4 and sum(order_cost)*0.05>=100 then sum(order_cost)*0.05 when avg(del.rate)<4 and sum(order_cost)*0.05<100 then 100
           when avg(del.rate)>=4 and avg(del.rate)<4.5 and sum(order_cost)*0.07>=150 then sum(order_cost)*0.07 when avg(del.rate)>=4 and avg(del.rate)<4.5 and sum(order_cost)*0.07<150 then 150
           when avg(del.rate)>=4.5 and avg(del.rate)<4.9 and sum(order_cost)*0.08>=175 then sum(order_cost)*0.08 when avg(del.rate)>=4.5 and avg(del.rate)<4.9 and sum(order_cost)*0.08<175 then 175
           when avg(del.rate)>=4.9 and sum(order_cost)*0.1>=200 then sum(order_cost)*0.1 when avg(del.rate)>=4.9 and sum(order_cost)*0.1<200 then 200
       end as cour_sum from dds.deliveries del left join dds.dm_orders ord on ord.order_key=del.order_id group by 1
)
insert into cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
select cour.courier_id, courier_name, extract(year from t.ts) as settlement_year,
       extract(month from del.order_ts) as settlement_month,
       count(order_id) as orders_count, sum(order_cost) as orders_total_sum, avg(del.rate)::decimal(4,2) as rate_avg,
       sum(order_cost) * 0.25 as order_processing_fee,
       cour_sum as courier_order_sum,
       sum(del.tip_sum) as courier_tip_sum,
       cour_sum + sum(del.tip_sum) * 0.95 as courier_reward_sum
from dds.deliveries del left join dds.couriers cour using (courier_id)
    left join dds.dm_orders d_ord on d_ord.order_key=del.order_id
    join dds.dm_timestamps t on d_ord.timestamp_id=t.id
    join courier_order_sum os on os.courier_id=del.courier_id
where order_status='CLOSED'
group by 1,2,3,4,9;