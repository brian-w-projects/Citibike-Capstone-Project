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
GROUP BY DATE(start), HOUR(start)