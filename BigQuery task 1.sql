SELECT timestamp_micros(event_timestamp) as event_timestamp,
user_pseudo_id,
(select evpar.value.int_value from bpd_ev.event_params evpar where evpar.key = 'ga_session_id') as session_id,
event_name,
 geo.country as country,
  device.category as device_category,
   traffic_source.source as sourse,
    traffic_source.medium as medium,
     traffic_source.name as campaign
 FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_2021*` bpd_ev
 Where event_name in ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info', 'purchase');