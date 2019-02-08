SELECT 
    DATE(start) AS 'date', 
    YEAR(start) AS 'year',
    MONTH(start) AS 'month',
    DAYOFWEEK(start) AS 'day_of_week',
    HOUR(start) AS 'hour', 
    ROUND(COS(HOUR(start) * PI() / 12), 5) AS 'cos_hour', 
    ROUND(SIN(HOUR(start) * PI() / 12), 5) AS 'sin_hour', 
    COUNT(*) AS 'rides'
FROM citibike.rides
GROUP BY DATE(start), HOUR(start);

#BIG QUERY SQL QUERY
SELECT
  r.date,
  r.hour,
  EXTRACT(MONTH FROM r.date) AS month,
  EXTRACT(DAYOFWEEK FROM r.date) AS day_of_week,
  ROUND(COS(r.hour) * ACOS(-1) / 12, 5) AS cos_hour,
  ROUND(SIN(r.hour) * ACOS(-1) / 12, 5) AS sin_hour,
  sunrise,
  icon,
  precip_prob,
  temperature,
  humidity,
  wind_speed,
  r.rides
FROM (
  SELECT
    EXTRACT(DATE FROM start_time) AS date,
    EXTRACT(HOUR FROM start_time) AS hour,
    COUNT(start_time) AS rides
  FROM `capstone-231016.capstone.rides`
  GROUP BY date, hour) AS r
JOIN (
  SELECT
    EXTRACT(DATE FROM timestamp) AS date,
    EXTRACT(HOUR FROM timestamp) AS hour,
    sunrise,
    icon,
    precip_prob,
    temperature,
    humidity,
    wind_speed
  FROM `capstone-231016.capstone.weather` ) AS w
ON w.date = r.date AND w.hour = r.hour
ORDER BY r.date, r.hour;
