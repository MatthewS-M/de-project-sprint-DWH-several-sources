Для реализации витрины будут необходимы следующие поля: 

|   |   |   |   |   |   |   |   |   |   |   |
|---|---|---|---|---|---|---|---|---|---|---|
|courier_id|courier_name|settlement_year|settlement_month|orders_count|orders_total_sum|rate_avg|order_processing_fee|courier_order_sum|courier_tip_sum|courier_reward_sum|

Для построения витрины данных с перечисленными выше атрибутами следует изначально извлечь данные из API и выгрузить их в сыром виде в staging слой as-is, то есть они должны оставаться в том же JSON формате. Далее необходимо преобразовать и перенести эти данные в Detailed Data Store, где данные будут уже разбиты на определенные колонки соответствующего типа.
В слое DDS уже находятся следующие нужные для витрины таблицы: `dm_orders`, `dm_timetamps`. Для построения витрины эти сущности надо дополнить (новыми колонками) и добавить новые сущности: `deliveries`, `couriers`. Таблица `dm_restaurants` содержит актуальные данные по все четырем ресторанам (было проверено выгрузкой из API, что новых ресторанов не нашлось), поэтому было принято решение оставить эту таблицу без изменения. 

DDL запросы для дополнения таблицы `dm_orders`:
```postgresql
alter table dds.dm_orders add column courier_id text references stg.couriers(courier_id);  
update dds.dm_orders ord set courier_id=d.courier_id from stg.deliveries d where d.order_id=ord.order_key;  
  
alter table dds.dm_orders add column order_cost numeric(14,2);  
update dds.dm_orders ord set order_cost=cast(s.object_value::json ->> 'cost' as numeric(14,2)) from stg.ordersystem_orders as s where s.object_id=ord.order_key;
```
Новые таблицы в слое DDS, необходимые для заполнения витрины: `dds.couriers`,`dds.deliveries`. Данные в нее переносятся из staging слоя соответствующих сущностей. Происходит извлечение каждого ключа в качестве наименования атрибута и присвоение ему значения из формата JSON.
