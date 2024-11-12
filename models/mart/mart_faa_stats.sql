WITH mart_faa_stats AS (
    SELECT * 
    FROM {{ref('prep_flights')}} )
    mart_faa_stats_origin AS(
        select
        	origin as faa,
        	count(sched_dep_time) as dep_planned,
        	count(*) as unique_dep,
        	sum(cancelled) as cancelled_dep,
        	sum(diverted) as diverted_dep,
        	count(dep_time) as occured_dep,
        	count(distinct tail_number) as unique_tails_dep,
        	count(distinct airline) as unique_airlines_dep
        FROM prep_flights
        group by origin
    ),
   mart_faa_stats_arr as(
   select
        	dest as faa,
        	count(sched_dep_time) as arr_planned,
        	count(*) as unique_arr,
        	sum(cancelled) as cancelled_arr,
        	sum(diverted) as diverted_arr,
        	count(dep_time) as occured_arr,
        	count(distinct tail_number) as unique_tails_arr,
        	count(distinct airline) as unique_airlines_arr
        FROM prep_flights
        group by dest),
   mart_faa_stats_air as(
   select
   		faa,
   		city,
   		country,
   		name
   from prep_airports),
 totals as(
 	select
 		dp.faa,
 		case when ar.unique_arr >= dp.unique_dep then ar.unique_arr else dp.unique_dep end as total_dep_arr,
 		dp.dep_planned as total_planned,
 		(dp.cancelled_dep + ar.cancelled_arr) as total_cancelled,
 		(dp.diverted_dep + ar.diverted_arr) as total_diverted,
 		count(dp.occured_dep)
 	from mart_faa_stats_origin dp
 	join mart_faa_stats_arr ar
 	on dp.faa = ar.faa
 	group by dp.faa, total_dep_arr, total_planned, total_cancelled, total_diverted)
 select
 		city,
 		country,
 		name,
  		t.*
  from totals t
  join mart_faa_stats_air ap
  on t.faa = ap.faa;