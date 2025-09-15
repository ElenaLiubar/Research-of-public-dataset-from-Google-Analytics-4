WITH path_table AS (select regexp_extract((select value.string_value from unnest(event_params) where key = 'page_location'), r'^https?://[^/]+(/[^?#]*)') as page_path,
user_pseudo_id || (select value.int_value from unnest (event_params) where key = 'ga_session_id') as user_session_id,
event_name
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_2020*`
WHERE event_name in ('session_start')),
purchase_table AS (select user_pseudo_id || (select value.int_value from unnest (event_params) where key = 'ga_session_id') as user_session_id,
event_name
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_2020*`
WHERE event_name in ('purchase')),
common_table AS (SELECT page_path, path_table.user_session_id as user_session_start,purchase_table.user_session_id as user_session_purch
FROM path_table
LEFT JOIN purchase_table
ON path_table.user_session_id =  purchase_table.user_session_id)
SELECT page_path, 
count (distinct user_session_start) AS user_sessions_start_count,
count (distinct user_session_purch) AS user_sessions_purch_count,
SAFE_DIVIDE(count (distinct user_session_purch), count (distinct user_session_start)) AS purch_conver
FROM common_table
GROUP BY 1;