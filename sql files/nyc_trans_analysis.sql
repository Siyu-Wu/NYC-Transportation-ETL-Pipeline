use DATABASE docker_nyc_trans_datamart;

select start_locationid,end_locationid, avg(duration_diff)
from trip_fact
group by start_locationid,end_locationid;