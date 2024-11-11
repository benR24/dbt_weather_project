WITH flights_one_month AS (
        SELECT * 
        FROM {{ref('staging_flights_one_month')}}
    ),
  flights_cleaned AS(
        SELECT flight_date::DATE
                ,TO_CHAR(dep_time, 'fm0000')::TIME AS dep_time
                ,to_char(sched_dep_time, 'fm0000')::time AS sched_dep_time
                ,dep_delay
    		    ,(dep_delay * '1 minute'::INTERVAL) AS dep_delay_interval
                ,to_char(arr_time, 'fm0000')::TIME AS arr_time
                ,to_char(sched_arr_time, 'fm0000')::TIME AS sched_arr_time
                ,arr_delay
                ,(arr_delay * '1 minute'::INTERVAL) AS arr_delay_interval
                ,airline
                ,tail_number
                ,flight_number
                ,origin
                ,dest
                ,air_time
                ,(air_time * '1 minute'::INTERVAL) AS air_time_interval
                ,actual_elapsed_time
                ,(actual_elapsed_time * '1 minute'::INTERVAL) AS actual_elapsed_time_interval
                ,(distance * 0.621371)::NUMERIC(6,2) AS distance_km
                ,cancelled
                ,diverted
        FROM flights_one_month
    )
    SELECT * FROM flights_cleaned