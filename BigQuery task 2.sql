WITH user_info_table AS (SELECT date(timestamp_micros(event_timestamp)) as event_date, 
traffic_source.source as sourse, 
traffic_source.medium as medium, 
traffic_source.name as campaign,
user_pseudo_id || (select value.int_value from unnest (event_params) where key = 'ga_session_id') as user_session_id,
event_name
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE event_name in ('session_start', 'add_to_cart', 'begin_checkout', 'purchase')),
count_events_table AS (SELECT event_date,sourse, medium, campaign,
count (distinct user_session_id) AS user_sessions_count,
count(distinct case when event_name = 'add_to_cart' then user_session_id end) AS count_add_to_cart,
count(distinct case when event_name = 'begin_checkout' then user_session_id end) AS count_begin_checkout,
count(distinct case when event_name = 'purchase' then user_session_id end) AS count_purchase,
count(distinct case when event_name = 'session_start' then user_session_id end) AS count_session_start
FROM user_info_table
GROUP BY 1,2,3,4)
SELECT event_date,sourse, medium, campaign, user_sessions_count,
count_add_to_cart/count_session_start AS visit_to_cart,
count_begin_checkout/count_session_start AS visit_to_checkout,
count_purchase/count_session_start AS visit_to_purchase
FROM count_events_table;