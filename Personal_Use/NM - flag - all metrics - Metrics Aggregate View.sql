-- AWR - flag - all metrics - Metrics Aggregate View.sql 
-- updated 21-Sep-2018 RDCornejo
-- DOPA Process -- Metrics Aggreegate View
-- -------------------------------------------------------------------------------------------
-- This work is offered by the author as a professional curtesy, “as-is” and without warranty.  
-- The author disclaims any liability for damages that may result from using this code.
-- -------------------------------------------------------------------------------------------
-- ============================================================
-- 08-Jun-2018 adjusting date ranges ect.
-- 07-Jun-2018 added Q1 and Q3 of IRQ to be variable
-- 18-Apr-2018 implementing the removal of outliers in the normal range calculation
-- 17-Apr-2018 implemented refactored taxonomy 
-- 02-Jan-2018 added some debug code
-- 12-Dec-2017 added subsetting on hour of day
-- 20-Nov-2017 Added expanded taxonomy
-- 08-Aug-2017 Updated for some comments
-- 26-Oct-2017 defalut :dba_hist_latch to N because it usually takes too long
-- dba_hist_sys_time_model
-- dba_hist_sysstat
-- dba_hist_osstat
-- dba_hist_iostat_function
-- dba_hist_sysmetric_summary
-- dba_hist_system_event
-- dba_hist_latch 
-- -  A latch is a lightweight lock used to protect a data structure. latches = locks, locks = serialization device, serialization devices = less scalable, slower. 

-- not instrumented yet:
--      dba_hist_waitstat 	cumulative stats on block contention	CLASS, WAIT_COUNT , TIME
--      dba_hist_undostat 	
--      <custom queries>	e.g. DBA_ADVISOR% ; DBA_HIST_SQLSTAT ; blocked sessions ...

-- -------------------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------------------
-- Metrics Aggregate View
-- -------------------------------------------------------------------------------------------------------------
-- -------------------------------------------------------------------------------------------------------------
with taxonomy as
(
select taxonomy_type, stat_source, metric_name, category,  sub_category 
from metric_Taxonomy
where upper(taxonomy_type) like upper(nvl(:taxonomy_type, 'Infrastructure'))
  and category like nvl(:category, category)
  and sub_category like nvl(:sub_category, sub_category)
)
-- select * from taxonomy;  -- testing thus far
, snaps_to_use as
(
select distinct snap_id , begin_interval_time, end_interval_time
from "SNAPSHOT" 
where 1=1
  and decode(:stats_days_back_only_Y_N,'Y', begin_interval_time, trunc(sysdate-:stats_days_back) ) >= trunc(sysdate-:stats_days_back)
    and (
       trunc(begin_interval_time, 'HH') between trunc(to_date(nvl(:allint_st_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
                                   and trunc(to_date(nvl(:allint_end_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
      )

)
-- select * from snaps_to_use order by snap_id;  -- testing thus far
, snaps_for_interval as
(
select distinct snap_id , begin_interval_time, end_interval_time
from "SNAPSHOT" 
where 1=1
  and (
       trunc(begin_interval_time, 'HH') between trunc(to_date(nvl(:intrvl_st_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
                                   and trunc(to_date(nvl(:intrvl_end_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
      )
/*  and    (to_number(to_char(begin_INTERVAL_TIME, 'HH24')) between nvl(:stats_begin_hour, 0) and  nvl(:stats_end_hour, 24) 
       or to_number(to_char(begin_INTERVAL_TIME, 'HH24')) between nvl(:stats_begin_hour2, nvl(:stats_begin_hour, 0)) and  nvl(:stats_end_hour2, nvl(:stats_end_hour, 24))) */
)
-- select * from snaps_for_interval order by snap_id; -- testing thus far
, snaps_for_normal_ranges as
(
select distinct snap_id , begin_interval_time, end_interval_time
from "SNAPSHOT" 
where 1=1
  and (
       trunc(begin_interval_time, 'HH') between trunc(to_date(nvl(:normRng_st_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
                                   and trunc(to_date(nvl(:normRng_end_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
      )
/*  and    (to_number(to_char(begin_INTERVAL_TIME, 'HH24')) between nvl(:stats_begin_hour, 0) and  nvl(:stats_end_hour, 24) 
       or to_number(to_char(begin_INTERVAL_TIME, 'HH24')) between nvl(:stats_begin_hour2, nvl(:stats_begin_hour, 0)) and  nvl(:stats_end_hour2, nvl(:stats_end_hour, 24))) */
)
-- select * from snaps_for_normal_ranges order by snap_id;  -- testing thus far
, snaps_to_use_for_deltas as
(
select snap_id from snaps_to_use union select min(snap_id)-1 from snaps_to_use
)
-- select snap_id from snaps_to_use_for_deltas order by snap_id;  -- testing thus far
, snaps as
(
select count(distinct snap_id) intervals 
from snaps_for_interval 
)
-- select intervals from snaps;  -- testing thus far
, stat AS (
SELECT * FROM normalised_metrics
)
--select distinct stat_source from stat;
--select * from stat  /* where snap_id = (select min(snap_id) from stat)+2 and (metric_name like '%cache buffer chains%' or metric_name like '%TCP Socket (KGAS)%' or metric_name like '%Direct Reads%') */ order by snap_id, stat_source, metric_name; -- testing SQL up tp this point
, outliers as
( select iqr.*
, case when Q1 - (nvl(:iqr_factor, 1.5) * IQR) > 0 then Q1 - (nvl(:iqr_factor, 1.5) * IQR) else 0 end as lower_outlier
, Q3 + (nvl(:iqr_factor, 1.5) * IQR) as upper_outlier
from (select stat.*
, Percentile_Cont(nvl(:Q1_PERCENTILE,0.25)) WITHIN GROUP (Order By average) OVER(partition by stat_source, metric_name) As Q1
, Percentile_Cont(nvl(:Q3_PERCENTILE,0.75)) WITHIN GROUP(Order By average) OVER(partition by stat_source, metric_name) As Q3
, Percentile_Cont(nvl(:Q3_PERCENTILE,0.75)) WITHIN GROUP(Order By average) OVER(partition by stat_source, metric_name) 
- Percentile_Cont(nvl(:Q1_PERCENTILE,0.25)) WITHIN GROUP(Order By average) OVER(partition by stat_source, metric_name) as IQR
from stat
where 1=1
  and snap_id in (select snap_id from snaps_to_use)
  and stat_source like nvl(:stat_source, stat_source)
) iqr
)
-- select * from outliers order by snap_id, stat_source, metric_name;
, normal_ranges as
(
select metric_id
, metric_name
, stat_source
, round(case when (AVG_average - (2 * STDDEV_average )) < 0 then min_average
       else (AVG_average - (2 * STDDEV_average))
  end) as lower_bound
, avg_average average_value
, round(case when (AVG_average + (2 * STDDEV_average )) >= max_average then max_average
       else AVG_average + (2 * STDDEV_average) 
  end) as upper_bound
, variance_average
, stddev_average
from
(
select metric_id
, metric_name
, round((VARIANCE(average) ), 1) variance_average
, round((STDDEV(average)  ), 1) stddev_average
, round((AVG(average)  )) avg_average
, round((MIN(average)  )) min_average
, round((MAX(average)  )) max_average
, stat_source
from outliers stat
where 1=1
  and (average > lower_outlier and average < upper_outlier) -- remove the outliers
  and snap_id <> min_snap_id
  and snap_id in (select snap_id from snaps_for_normal_ranges)
--  and trunc(begin_interval_time) between trunc(sysdate- nvl(:normal_ranges_days_back,8)) and trunc(sysdate)
--  and upper(metric_name) like upper(nvl(:metric_name, metric_name))
group by metric_id, metric_name, stat_source
)
)
-- select * from normal_ranges order by upper(metric_name); -- testing SQL up tp this point
, metrics as
(
select instance_name
, snap_id
, begin_interval_time as begin_time
, a.metric_name
, average 
, metric_id
, host_name
, version
, dbid
, instance_number
, a.stat_source
, taxonomy_type
, category
, sub_category
from stat a
, taxonomy b
where 1=1
  and a.stat_source = b.stat_source and a.metric_name = b.metric_name
  and snap_id <> min_snap_id
  and snap_id in (select snap_id from snaps_for_interval)
/*
  and decode(:days_back_only_Y_N,'Y', begin_interval_time, trunc(sysdate-:days_back) ) >= trunc(sysdate-:days_back)
  and (
       trunc(begin_interval_time, 'HH') between trunc(to_date(nvl(:sam_tm_str_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
                                   and trunc(to_date(nvl(:sam_tm_end_MM_DD_YYYY_HH24_MI, to_char(begin_interval_time, 'MM_DD_YYYY_HH24_MI')),'MM_DD_YYYY_HH24_MI') , 'HH')
      )
  and    (to_number(to_char(begin_INTERVAL_TIME, 'HH24')) between nvl(:begin_hour, 0) and  nvl(:end_hour, 24) 
       or to_number(to_char(begin_INTERVAL_TIME, 'HH24')) between nvl(:begin_hour2, nvl(:begin_hour, 0)) and  nvl(:end_hour2, nvl(:end_hour, 24)))
*/
)
-- select * from metrics order by snap_id; -- testing SQL up tp this point
, flags as
(
select instance_name
, host_name
, version
, snap_id
--, a.instance_number
--, a.dbid
, begin_time
--, end_time
, metrics.metric_name
--, minval
, average
, lower_bound
, upper_bound
--, round(100*(1 - ((upper_bound) / nullif(average,0)))) flag_ratio
, round((average - upper_bound) / (nullif(stddev_average, 0)), 2 ) flag_ratio
, case when average > upper_bound then 1 else 0 end as flag
, variance_average
, stddev_average
, average_value
, metrics.metric_id
, metrics.stat_source
, taxonomy_type
, category
, sub_category
from metrics
, normal_ranges
where 1=1
and metrics.metric_name = normal_ranges.metric_name
and metrics.metric_id = normal_ranges.metric_id
and metrics.stat_source = normal_ranges.stat_source
)
-- select * from flags; -- testing SQL up tp this point
, metrics_time_series_view as
(
select taxonomy_type
, category
, sub_category
, stat_source
, snap_id
--, begin_time
, to_char(begin_time,'YYYY-MM-DD HH24:MI') as begin_time
, metric_name
, average  
, lower_bound
, upper_bound
-- , flag_ratio
, flag_ratio
, CASE 
    when flag_ratio between  0.00 and  0.49 then  '*'  
    when flag_ratio between  0.50 and  0.99 then  '**'  
    when flag_ratio between  1.00 and  1.49 then  '***'  
    when flag_ratio between  1.50 and  1.99 then  '****'  
    when flag_ratio between  2.00 and  2.49 then  '*****'  
    when flag_ratio between  2.50 and  2.99 then  '******' 
    when flag_ratio between  3.00 and  3.49 then  '*******'  
    when flag_ratio between  3.50 and  3.99 then  '********'  
    when flag_ratio between  4.00 and  4.49 then  '*********'  
    when flag_ratio between  4.50 and  4.99 then  '**********' 
    when flag_ratio between  5.00 and  5.49 then  '***********'  
    when flag_ratio between  5.50 and  5.99 then  '************'  
    when flag_ratio between  6.00 and  6.49 then  '*************'  
    when flag_ratio between  6.50 and  6.99 then  '**************'  
    when flag_ratio between  7.00 and  7.49 then  '***************'  
    when flag_ratio between  7.50 and  7.99 then  '****************' 
    when flag_ratio between  8.00 and  8.49 then  '*****************'  
    when flag_ratio between  8.50 and  8.99 then  '******************'  
    when flag_ratio between  9.00 and  9.49 then  '*******************'  
    when flag_ratio between  9.50 and  9.99 then  '********************'  
    when flag_ratio between 10.00 and 10.99 then  '**********************'  
    when flag_ratio between 11.00 and 11.99 then  '************************'
    when flag_ratio between 12.00 and 12.99 then  '**************************'  
    when flag_ratio between 13.00 and 13.99 then  '****************************'  
    when flag_ratio between 14.00 and 14.99 then  '******************************'  
    when flag_ratio between 15.00 and 15.99 then  '********************************'    
    when flag_ratio is null                then null
    when flag_ratio < 0                    then null    
    else '******************** ********** **********' 
END as flag_eval
, flag
, variance_average
--, instance_name
--, host_name
--, version
, stddev_average
, average_value
--, metric_id
from flags
where 1=1
/* */
  and decode(:flagged_values_only_Y_N,'Y', 1, flag) = flag  -- if you want to see flagged values only, then include rows that are flagged above or below usual ranges
  and decode(:flagged_values_only_Y_N,'Y', flag_ratio, 999) >= nvl(:flag_ratio, 0.00) -- if you want to see flagged values only, include rows if average is x% bigger then the upper bound
--  and decode(:flagged_values_only_Y_N,'Y', variance_average, 1) >=  .1  -- if you want to see flagged values only, include rows if there is a variance in the values
/* */
--  and upper(metric_name) like upper(nvl(:metric_name_2, metric_name))
--  and stat_source like nvl(:stat_source, stat_source)
--  and category like nvl(:category, category)
--  and sub_category like nvl(:sub_category, sub_category)
order by instance_name, snap_id, stat_source, decode(:flagged_values_only_Y_N,'Y', flag_ratio, 1) desc, metric_id
)
-- select * from metrics_time_series_view; -- testing thus far
, metrics_aggregate_view as
(
select taxonomy_type, CATEGORY,SUB_CATEGORY,STAT_SOURCE, METRIC_NAME
, count(flag) flag_count
, (select intervals from snaps) intervals
, round(avg(average)) "AVG Flagged Values"
, min(LOWER_BOUND) lower_bound
, max(UPPER_BOUND) upper_bound
--, round(avg(flag_Ratio)) flag_Ratio
, round(avg(flag_ratio), 2) flag_ratio
, CASE 
    when round(avg(flag_ratio),2) between  0.00 and 0.49 then  '*'  
    when round(avg(flag_ratio),2) between  0.50 and 0.99 then  '**'  
    when round(avg(flag_ratio),2) between  1.00 and 1.49 then  '***'  
    when round(avg(flag_ratio),2) between  1.50 and 1.99 then  '****'  
    when round(avg(flag_ratio),2) between  2.00 and 2.49 then  '*****'  
    when round(avg(flag_ratio),2) between  2.50 and 2.99 then  '******' 
    when round(avg(flag_ratio),2) between  3.00 and 3.49 then  '*******'  
    when round(avg(flag_ratio),2) between  3.50 and 3.99 then  '********'  
    when round(avg(flag_ratio),2) between  4.00 and 4.49 then  '*********'  
    when round(avg(flag_ratio),2) between  4.50 and 4.99 then  '**********'  
    
    when round(avg(flag_ratio),2) between  5.00 and 5.49 then  '***********'  
    when round(avg(flag_ratio),2) between  5.50 and 5.99 then  '************'  
    when round(avg(flag_ratio),2) between  6.00 and 6.49 then  '*************'  
    when round(avg(flag_ratio),2) between  6.50 and 6.99 then  '**************'  
    when round(avg(flag_ratio),2) between  7.00 and 7.49 then  '***************'  
    when round(avg(flag_ratio),2) between  7.50 and 7.99 then  '****************' 
    when round(avg(flag_ratio),2) between  8.00 and 8.49 then  '*****************'  
    when round(avg(flag_ratio),2) between  8.50 and 8.99 then  '******************'  
    when round(avg(flag_ratio),2) between  9.00 and 9.49 then  '*******************'  
    when round(avg(flag_ratio),2) between  9.50 and 9.99 then  '********************'  
    when round(avg(flag_ratio),2) is null                then null
    when round(avg(flag_ratio),2) < 0                    then null    
    else '******************** ********** **********' 

END as flag_eval

, avg(average_value) "AVG All"
-- , SNAP_ID,BEGIN_TIME,FLAG_RATIO,FLAG_EVAL,FLAG,VARIANCE_AVERAGE,STDDEV_AVERAGE,AVERAGE_VALUE
from metrics_time_series_view
where 1=1
-- and flag=1 -- look at flagged values only
  and decode(:flagged_values_only_Y_N,'Y', 1, flag) = flag  -- if you want to see flagged values only, then include rows that are flagged above or below usual ranges
  and decode(:flagged_values_only_Y_N,'Y', flag_ratio, 999) >= nvl(:flag_ratio, 0.00) -- if you want to see flagged values only, include rows if average is x% bigger then the upper bound
group by taxonomy_type, CATEGORY,SUB_CATEGORY,STAT_SOURCE, METRIC_NAME
order by 6 desc, 11 desc, taxonomy_type, category, sub_category, stat_source, metric_name
)
--select * from metrics_aggregate_view
, category_count_view as
(select taxonomy_type, category, count(distinct stat_source||':'||metric_name) category_count 
from metrics_time_series_view
group by taxonomy_type, category
)
-- select * from metrics_time_series_view;
select * from metrics_aggregate_view;
--select * from category_count_view;

