-- AWR - flag - all metrics - Category Count View.sql
-- updated 21-Sep-2018 RDCornejo
-- DOPA Process -- Category Count View
-- -------------------------------------------------------------------------------------------
-- This work is offered by the author as a professional curtesy, “as-is” and without warranty.  
-- The author disclaims any liability for damages that may result from using this code.
-- --------------------------------------------------------------------------------------------- 
---------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------
-- category count view:
-- ---------------------------------------------------------------------------------------------------------------------------
-- ---------------------------------------------------------------------------------------------------------------------------
with  latch as
(
select * from dba_hist_latch a where snap_id in (select snap_id from snaps_to_use_for_deltas) 
)
-- select * from latch where latch_name like 'cache %' order by latch_name, 1;  -- testing thus far
, unpivot_latch as
(
      select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' gets' metric_name, gets cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' misses' metric_name,  misses cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' sleeps' metric_name,  sleeps cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' immediate_gets' metric_name, immediate_gets cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' immediate_misses' metric_name,  immediate_misses cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' spin_gets' metric_name, spin_gets cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' sleep1' metric_name,  sleep1 cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' sleep2' metric_name,  sleep2 cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' sleep3' metric_name,  sleep3 cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' sleep4' metric_name,  sleep4 cumulative_value , a.dbid, a.instance_number from latch a
union select a.snap_id, a.latch_hash metric_id, LEVEL# || ': ' || latch_name || ' wait_time' metric_name,  wait_time cumulative_value , a.dbid, a.instance_number from latch a
)
-- select * from unpivot_latch where metric_name like '%cache buffer%' order by 1,2; -- testing thus far
, system_event as
(
select * from dba_hist_system_event a where snap_id in (select snap_id from snaps_to_use_for_deltas) 
)
-- select * from system_event ;  -- testing thus far
, unpivot_system_event as
(
      select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_waits' metric_name, total_waits cumulative_value , a.dbid, a.instance_number from system_event a
union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_timeouts' metric_name,  total_timeouts cumulative_value , a.dbid, a.instance_number from system_event a
union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' time_waited_micro' metric_name,  time_waited_micro cumulative_value , a.dbid, a.instance_number from system_event a
-- comment out since _fg versions of the metric have the same values as the non-_fg version
--union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_waits_fg' metric_name,  total_waits_fg cumulative_value , a.dbid, a.instance_number from system_event a
--union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' total_timeouts_fg' metric_name,  total_timeouts_fg cumulative_value , a.dbid, a.instance_number from system_event a
--union select a.snap_id, a.event_id metric_id, wait_class || ': ' || event_name || ' time_waited_micro_fg' metric_name,  time_waited_micro_fg cumulative_value , a.dbid, a.instance_number from system_event a
)
, iostat_function as
(
select * from dba_hist_iostat_function a where snap_id in (select snap_id from snaps_to_use_for_deltas) 
)
-- select * from iostat_function where function_name = 'Direct Reads' order by snap_id;
, unpivot_iostat_function as
(
      select a.snap_id, a.function_id metric_id, function_name || ' small_read_megabytes' metric_name, small_read_megabytes cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' small_write_megabytes' metric_name,  small_write_megabytes cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' large_read_megabytes' metric_name, large_read_megabytes  cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' large_write_megabytes' metric_name, large_write_megabytes cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' small_read_reqs' metric_name, small_read_reqs cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' small_write_reqs' metric_name, small_write_reqs cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' large_read_reqs' metric_name, large_read_reqs cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' large_write_reqs' metric_name, large_write_reqs cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' number_of_waits' metric_name, number_of_waits cumulative_value , a.dbid, a.instance_number from iostat_function a
union select a.snap_id, a.function_id metric_id, function_name || ' wait_time' metric_name, wait_time cumulative_value , a.dbid, a.instance_number from iostat_function a
)
--select * from unpivot_iostat_function where metric_name like 'Direct Reads%' order by snap_id;
, stat as
(
select /*+ MATERIALIZE */ instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, a.stat_id metric_id
, stat_name metric_name
,   nvl(decode(greatest(value, nvl(lag(value) over
      (partition by a.dbid, a.instance_number, a.stat_name order by a.snap_id),0)),
    value,
    value - lag(value) over
      (partition by a.dbid, a.instance_number, a.stat_name order by a.snap_id),value), 0) average
  , a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.stat_name) min_snap_id
, 'dba_hist_sys_time_model' stat_source
from dba_hist_sys_time_model a
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_sys_time_model, 'Y') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use_for_deltas) 
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.stat_name) like upper(nvl(:metric_name, a.stat_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
union
select instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, a.stat_id metric_id
, stat_name metric_name
,   nvl(decode(greatest(value, nvl(lag(value) over
      (partition by a.dbid, a.instance_number, a.stat_name order by a.snap_id),0)),
    value,
    value - lag(value) over
      (partition by a.dbid, a.instance_number, a.stat_name order by a.snap_id),value), 0) average
  , a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.stat_name) min_snap_id
, 'dba_hist_sysstat' stat_source
from dba_hist_sysstat a
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_sysstat, 'Y') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use_for_deltas)
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.stat_name) like upper(nvl(:metric_name, a.stat_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
union
select instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, a.stat_id metric_id
, stat_name metric_name
,   nvl(decode(greatest(value, nvl(lag(value) over
      (partition by a.dbid, a.instance_number, a.stat_name order by a.snap_id),0)),
    value,
    value - lag(value) over
      (partition by a.dbid, a.instance_number, a.stat_name order by a.snap_id),value), 0) average
  , a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.stat_name) min_snap_id
, 'dba_hist_osstat' stat_source
from dba_hist_osstat a
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_osstat, 'Y') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use_for_deltas)
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.stat_name) like upper(nvl(:metric_name, a.stat_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
  and stat_name not in ('x'
,'NUM_CPUS'
,'NUM_CPU_CORES'
,'NUM_CPU_SOCKETS'
,'PHYSICAL_MEMORY_BYTES'
,'TCP_SEND_SIZE_DEFAULT'
,'TCP_SEND_SIZE_MAX'
,'TCP_RECEIVE_SIZE_DEFAULT'
,'TCP_RECEIVE_SIZE_MAX')
union
select instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, metric_id
, metric_name
,   nvl(decode(greatest(cumulative_value, nvl(lag(cumulative_value) over
      (partition by a.dbid, a.instance_number, a.metric_name order by a.snap_id),0)),
    cumulative_value,
    cumulative_value - lag(cumulative_value) over
      (partition by a.dbid, a.instance_number, a.metric_name order by a.snap_id),cumulative_value), 0) average
  , a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.metric_name) min_snap_id
, 'dba_hist_iostat_function' stat_source
from unpivot_iostat_function a
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_iostat_function, 'Y') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use_for_deltas)
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.metric_name) like upper(nvl(:metric_name, a.metric_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
union
select instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, metric_id
, metric_name
, round(average) average
, a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.metric_name) min_snap_id
, 'dba_hist_sysmetric_summary' stat_source
from dba_hist_sysmetric_summary a 
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_sysmetric_summary, 'Y') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use)
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.metric_name) like upper(nvl(:metric_name, a.metric_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
union
select instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, metric_id
, metric_name
,   nvl(decode(greatest(cumulative_value, nvl(lag(cumulative_value) over
      (partition by a.dbid, a.instance_number, a.metric_name order by a.snap_id),0)),
    cumulative_value,
    cumulative_value - lag(cumulative_value) over
      (partition by a.dbid, a.instance_number, a.metric_name order by a.snap_id),cumulative_value), 0) average
  , a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.metric_name) min_snap_id
, 'dba_hist_system_event' stat_source
from unpivot_system_event a
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_system_event, 'Y') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use_for_deltas)
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.metric_name) like upper(nvl(:metric_name, a.metric_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
union 
select instance_name
, host_name
, version
, a.snap_id
, trunc(b.begin_interval_time, 'HH24') begin_interval_time
, metric_id
, metric_name
,   nvl(decode(greatest(cumulative_value, nvl(lag(cumulative_value) over
      (partition by a.dbid, a.instance_number, a.metric_name order by a.snap_id),0)),
    cumulative_value,
    cumulative_value - lag(cumulative_value) over
      (partition by a.dbid, a.instance_number, a.metric_name order by a.snap_id),cumulative_value), 0) average
  , a.dbid, a.instance_number
, min(a.snap_id) over (partition by a.dbid, a.instance_number, a.metric_name) min_snap_id
, 'dba_hist_latch' stat_source
from unpivot_latch a
, dba_hist_snapshot b
, gv$instance c
, gv$database d
where 1=1
  and nvl(:dba_hist_latch, 'N') = 'Y'
  and a.snap_id in (select snap_id from snaps_to_use_for_deltas)
  and a.snap_id = b.snap_id
  and a.dbid = b.dbid
  and a.instance_number=b.instance_number
  and b.instance_number=c.instance_number
  and a.instance_number=c.instance_number
  and c.instance_number = d.inst_id 
  and a.instance_number = d.inst_id 
  and d.dbid = a.dbid
  and upper(a.metric_name) like upper(nvl(:metric_name, a.metric_name))
  and a.instance_number = nvl(:inst_id, a.instance_number)
)
select * from stat;