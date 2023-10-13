{{
    config(
    materialized='table',
    cluster_by = ['customer_identifier'],
    )
}}

with rfm as (select * from {{ ref('user_summary') }}

)

select 
case 
  when CENTROID_ID = 1 then 'Inactive'
  when CENTROID_ID = 2 then 'Loyal'
  when CENTROID_ID = 3 then 'Needs Nurturing'
  when CENTROID_ID = 4 then 'High Potential'
  when CENTROID_ID = 5 then 'Low Potential'
  when CENTROID_ID = 6 then 'Champions'
end as segments,
current_timestamp() as snapshot_at,
* except (nearest_centroids_distance, CENTROID_ID)

FROM ML.PREDICT(MODEL `ellabache-singleview.models.segmentation`,
(select * from rfm))
