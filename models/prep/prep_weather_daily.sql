WITH daily_data AS (
        SELECT * 
        FROM {{ref('staging_weather_daily')}}
    ),
    add_features AS (
        SELECT *
    		, date_part('day', date) AS date_day
    		, date_part('month', date) AS date_month
    		, date_part('year', date) AS date_year
    		, EXTRACT(week from date) AS cw
    		, EXTRACT(month from date) AS month_name
    		, EXTRACT(day from date) AS weekday
        FROM daily_data 
    ),
    add_more_features AS (
        SELECT *
    		, (CASE 
    			WHEN month_name in (12,1,2) THEN 'winter'
    			WHEN month_name in (3,4,5 ) THEN 'spring'
                WHEN month_name in (6,7,8) THEN 'summer'
                WHEN month_name in (9,10,11) THEN 'autumn'
    		END) AS season
        FROM add_features
    )
    SELECT *
    FROM add_more_features
    ORDER BY date