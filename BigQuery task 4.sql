WITH info_table as (SELECT user_pseudo_id || (select value.int_value from unnest (event_params) where key = 'ga_session_id') as user_session_id,
max (coalesce (cast((select value.string_value from unnest (event_params) where key = 'session_engaged') as integer),
(select value.int_value from unnest (event_params) where key = 'session_engaged'), 0)) as user_engaged,
sum (coalesce((select value.int_value from unnest (event_params) where key = 'engagement_time_msec'), 0)) as user_session_time,
max (case when event_name = 'purchase' then 1 else 0 end) as purchase
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
GROUP BY 1)
SELECT 
corr (user_engaged, purchase) as eng_purch_corr,
corr (user_session_time, purchase) as time_purch_corr
FROM info_table;