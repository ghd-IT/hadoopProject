1、create table if not exists dw_dp(
s_time bigint,
pl string,
p_url string,
u_ud string,
u_sd string
)
partitioned by(month string,day string)
row format delimited
fields terminated by ' '
stored as orc;



insert into table dw_dp partition(month='11',day='02')
select s_time,pl,p_url,u_ud,u_sd
from logs
where month="11" and day ="02"
and en ='e_pv'
;

create table dwa_dp(
pl string,
dt string,
col string,
ct int
);

CREATE TABLE stats_view_depth (
platform_dimension_id int,
data_dimension_id int,
kpi_dimension_id int,
pv1 int,
pv2 int,
pv3 int,
pv4 int,
pv5_10 int,
pv10_30 int,
pv30_60 int,
pv60pluss int,
created string
);

insert overwrite table dwa_dp
select pl,dt,pv,count(distinct u_ud)
from  (
select from_unixtime(cast(dd.s_time/1000 as bigint),"yyyy-MM-dd") dt,
dd.pl pl,
(case when count(dd.p_url)=1 then 'pv1'
when count(dd.p_url)=1 then 'pv1'
when count(dd.p_url)=2 then 'pv2'
when count(dd.p_url)=3 then 'pv3'
when count(dd.p_url)=4 then 'pv4'
when count(dd.p_url)<=10 then 'pv5_10'
when count(dd.p_url)<=30 then 'pv10_30'
when count(dd.p_url)<=60 then 'pv30_60'
else 'pv60pluss'
end )as pv,
dd.u_ud u_ud
from dw_dp dd
where dd.pl is not null
and dd.p_url is not null
and dd.month="11" and dd.day ="02"
group by pl,s_time,u_ud
) as tmp
group by pl,dt,pv
;


insert overwrite table stats_view_depth
select date_convert(dt),platform_convert(pl),2,
sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),
sum(pv30_60),sum(pv60pluss),dt
from(
select pl as pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col ="pv1" union all
select pl as pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col ="pv2" union all
select pl as pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col ="pv3" union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col ="pv4" union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col ="pv5_10" union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4, 0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col ="pv10_30" union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from dwa_dp where col ="pv30_60" union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from dwa_dp where col ="pv60pluss" union all
select 'all' as pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col ="pv1" union all
select 'all' as pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col ="pv2" union all
select 'all' as pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col ="pv3" union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col ="pv4" union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col ="pv5_10" union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4, 0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col ="pv10_30" union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from dwa_dp where col ="pv30_60" union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from dwa_dp where col ="pv60pluss"
)tmp
group by pl,dt
;


sqoop export --connect jdbc:mysql://hdp01:3306/test \
--username hive --password 123456 --table stats_view_depth \
--export-dir 'hdfs://192.168.152.10:9000/user/hive/warehouse/gp1809.db/stats_view_depth/*' \
--input-fields-terminated-by '\001' \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,kpi_dimension_id

