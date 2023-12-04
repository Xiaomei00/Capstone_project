
-- join all table
CREATE TABLE capstone-project-402112.cyclistic_bike.April_july AS (  

  SELECT * FROM capstone-project-402112.cyclistic_bike.april 
  UNION ALL
  SELECT * FROM capstone-project-402112.cyclistic_bike.May
  UNION ALL
  SELECT * FROM capstone-project-402112.cyclistic_bike.Jun
  UNION ALL
  SELECT * FROM capstone-project-402112.cyclistic_bike.July
)
-- take only non null 
CREATE table capstone-project-402112.cyclistic_bike.clean_no_null as(
  SELECT * 
  FROM capstone-project-402112.cyclistic_bike.April_july
  WHERE 
    start_station_name IS NOT NULL AND
    start_station_id IS NOT NULL AND
    end_station_name IS NOT NULL AND
    end_station_id IS NOT NULL AND
    end_lat IS NOT NULL AND 
    end_lng IS NOT NULL 
) 

--check null each coloumn
SELECT 
  sum(CASE WHEN ride_id is NULL then 1 else 0 end) as ride_id,
  sum(CASE WHEN start_station_name is NULL THEN 1 ELSE 0 end) as start_name,
  sum(CASE WHEN rideable_type is NULL THEN 1 ELSE 0 end) as ride_type,
  sum(CASE WHEN started_at is NULL THEN 1 ELSE 0 end) as start_at ,
  sum(CASE WHEN ended_at is NULL THEN 1 ELSE 0 end) as end_at,
  sum(CASE WHEN start_station_id is NULL THEN 1 ELSE 0 end) as start_id,
  sum(CASE WHEN end_station_name is NULL THEN 1 ELSE 0 end)as end_name,
  sum(CASE WHEN end_station_id is NULL THEN 1 ELSE 0 end) as end_id,
  sum(CASE WHEN start_lat is NULL THEN 1 ELSE 0 end) as start_lat,
  sum(CASE WHEN end_lat is NULL THEN 1 ELSE 0 end) as end_lat,
  sum(CASE WHEN end_lng is NULL THEN 1 ELSE 0 end) as end_lng,
  sum(CASE WHEN start_lng is NULL THEN 1 ELSE 0 end) as start_lng,
  sum(CASE WHEN member_casual is NULL THEN 1 ELSE 0 end) as member_casual 
FROM capstone-project-402112.cyclistic_bike.April_july

-- transform and clean data 
SELECT 
    ride_id,
    rideable_type,
    start_station_name,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual as Category_member,
    started_at,
    CASE 
      WHEN (EXTRACT(DAYOFWEEK FROM started_at)) = 1 THEN "Sun"
      WHEN (EXTRACT(DAYOFWEEK FROM started_at)) = 2 THEN "Mon"
      WHEN (EXTRACT(DAYOFWEEK FROM started_at)) = 3 THEN "Tues"
      WHEN (EXTRACT(DAYOFWEEK FROM started_at)) = 4 THEN "Wed"
      WHEN (EXTRACT(DAYOFWEEK FROM started_at)) = 5 THEN "Thurs"
      WHEN (EXTRACT(DAYOFWEEK FROM started_at)) = 6 THEN "Fri"
      else "Sat" END as day_of_week,
    CASE 
      WHEN (EXTRACT(MONTH FROM started_at)) = 4 THEN "April"
      WHEN (EXTRACT(MONTH FROM started_at)) = 5 THEN "May"
      WHEN (EXTRACT(MONTH FROM started_at)) = 6 THEN "Jun"
      WHEN (EXTRACT(MONTH FROM started_at)) = 7 THEN "July"
      else "Unknown" END as Month,
    EXTRACT(DAY FROM started_at) AS DAY,  
    TIMESTAMP_DIFF(ended_at,started_at, minute) AS ride_length_m,
    FORMAT_TIMESTAMP("%H:%M:%S %p",started_at) as start_time,
    EXTRACT(HOUR FROM started_at) as hour

  FROM capstone-project-402112.cyclistic_bike.clean_no_null
  WHERE TIMESTAMP_DIFF(ended_at,started_at,minute) > 1 AND TIMESTAMP_DIFF (ended_at,started_at,hour) < 24 

--check duplicate
SELECT 
  COUNT(*) AS total_row_num,
  COUNT(DISTINCT(ride_id)) as disticnt_ride_id,
  (SELECT 
  DISTINCT(LENGTH(ride_id)) AS length_id

  FROM capstone-project-402112.cyclistic_bike.April_july) AS length_ride_id
  

FROM capstone-project-402112.cyclistic_bike.April_july


--check monthly use by category
SELECT 
  Month,
  Category_member,
  COUNT(*) as Total_USE_month
  
from capstone-project-402112.cyclistic_bike.clean_data
GROUP BY Month,Category_member
order by Month


-- category use bicycle the most
SELECT 
  COUNT(*) as Total_USE,
  Category_member
from capstone-project-402112.cyclistic_bike.clean_data
GROUP BY Category_member


--Average ride by category
SELECT 
  Category_member, 
  ROUND(AVG(ride_length_m),2) as average_minute
from capstone-project-402112.cyclistic_bike.clean_data
GROUP BY Category_member 

--count hourly use by category
SELECT 
  hour,
  Category_member,
  COUNT(hour) as Total_people,
  
from capstone-project-402112.cyclistic_bike.clean_data
GROUP BY hour, Category_member
ORDER BY hour DESC

--count daily use by category
SELECT 
  day_of_week,
  Category_member,
  COUNT(day_of_week) as Total_USE_in_week
  
from capstone-project-402112.cyclistic_bike.clean_data
GROUP BY day_of_week,Category_member
order by day_of_week 


--maximun how far go by category
SELECT 
  Category_member, 
  ROUND(MAX(ride_length_m),2) as average_minute
from capstone-project-402112.cyclistic_bike.clean_data
GROUP BY Category_member 


--minimum how far go by category
SELECT 
  Category_member, 
  ROUND(Min(ride_length_m),2) as average_minute
from capstone-project-402112.cyclistic_bike.clean_data
GROUP BY Category_member

--popular place they go by category
SELECT 
  count(*) as Total_route_use,
  Category_member,
  start_station_name,
  end_station_name
from capstone-project-402112.cyclistic_bike.clean_data
GROUP BY start_station_name, end_station_name, Category_member
order by start_station_name DESC 



--Which ride rype customer always use by category
SELECT 
  Category_member,
  rideable_type,
  COUNT(rideable_type) as type_of_bicycle
  
from capstone-project-402112.cyclistic_bike.clean_data
GROUP BY rideable_type,Category_member