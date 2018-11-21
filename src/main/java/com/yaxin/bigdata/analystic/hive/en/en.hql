create external table if not exists logs(
    ver string,
    s_time string,
    en string,
    u_ud string,
    u_mid string,
    u_sd string,
    c_time string,
    l string,
    b_iev string,
    b_rst string,
    p_url string,
    p_ref string,
    tt string,
    pl string,
    ip string,
    oid string,
    `on` string,
    cua string,
    cut string,
    pt string,
    ca string,
    ac string,
    kv_ string,
    du string,
    browserName string,
    browserVersion string,
    osName string,
    osVersion string,
    country string,
    province string,
    city string
    )
    partitioned by(month String,day string)
    row format delimited fields terminated by '\001'
    ;
    alter database hive  character set latin1
    load data inpath '/ods/11/02/' into table logs partition(month='11',day='02');
1、创建事件的基础维度类，不需要使用集合维度类对象

2、创建获取维度id的udf函数,并测试
create  function date_convert as 'com.yaxin.bigdata.analystic.hive.DateDimensionUdf' using jar 'hdfs://hdp01:9000/logs/udfjars/hadoopProject-1.0-SNAPSHOT.jar'
3、创建hive表映射每一天的数据，创建分区表
create table if not exists dw_en(
s_time bigint,
pl string,
ca string,
ac string
)
partitioned by(month string ,day string)
row format delimited fields terminated by ' '
stored as orc;
导入数据
from  logs
insert into table dw_en partition(month="11",day="02")
select s_time,pl,ca,ac
where month = "11" and day = "02";
4、创建最终结果表
CREATE external TABLE `stats_event` (
  `platform_dimension_id` int ,
  `date_dimension_id` int,
  `event_dimension_id` int,
  `times` int,
  `created` string
);

5、写 hql
导入数据到stats_event
with tmp as (
select from_unixtime(cast(de.s_time/1000 as bigint),"yyyy-MM-dd") dt,
de.pl pl,de.ca ca,de.ac ac
from dw_en de
where pl is not null
and de.month="11" and de.day= "02"
)
from (
select pl as pl,dt, ca as ca ,ac as ac,count(1) as ct from tmp group by pl,dt,ca,ac union all
select pl as pl,dt, ca as ca ,'all' as ac,count(1) as ct from tmp group by pl,dt,ca union all
select 'all' as pl,dt, ca as ca ,ac as ac,count(1) as ct from tmp group by dt,ca,ac union all
select 'all' as pl,dt, ca as ca ,'all' as ac,count(1) as ct  from tmp group by dt,ca union all
select pl as pl,dt, 'all' as ca ,'all' as ac,count(1) as ct  from tmp group by pl,dt union all
select 'all' as pl,dt, 'all' as ca ,'all' as ac,count(1) as ct  from tmp group by dt
) as tmp2
insert into stats_event
select date_convert(dt),platform_convert(pl),event_convert(ca,ac),sum(ct),dt
group by pl,dt,ca,ac
;

6、扩展维度，并将结果集导出到结果表中 union all

7、使用sqoop体育局将结果表导出到mysql
sqoop export --connect jdbc:mysql://hdp01:3306/test \
--username hive --password 123456 --table stats_event \
--export-dir 'hdfs://192.168.152.10:9000/user/hive/warehouse/gp1809.db/stats_event/*' \
--input-fields-terminated-by '\001' \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,event_dimension_id
;
8、将整个封装成shell脚本
判断时间，没有默认昨天的时间执行

#!/bin/bash

#./en.sh -n -m -d 2018-08-30
run_date=
until [$# -eq 0]
do
if [$1'x' = '-dx']
then
shift
run_date=$1
fi
shift
done

if [ ${#run_date} !=10]
then
run_date = `date -d "1 days ago" "+%Y-%m-%d"`
else
echo "$run_date"
fi
month=`date -d "$run_date" "+%m"`
day=`date -d "$run_date" "+%d"`
echo "final running date is:${run_date},${month},${day}"
############
#run hql statment
###########

with tmp as (
select from_unixtime(cast(de.s_time/1000 as bigint),"yyyy-MM-dd") dt,
de.pl pl,de.ca ca,de.ac ac
from dw_en de
where pl is not null
and de.month="${month}" and de.day= "${day}"
)
from (
select pl as pl,dt, ca as ca ,ac as ac,count(1) as ct from tmp group by pl,dt,ca,ac union all
select pl as pl,dt, ca as ca ,'all' as ac,count(1) as ct from tmp group by pl,dt,ca union all
select 'all' as pl,dt, ca as ca ,ac as ac,count(1) as ct from tmp group by dt,ca,ac union all
select 'all' as pl,dt, ca as ca ,'all' as ac,count(1) as ct  from tmp group by dt,ca union all
select pl as pl,dt, 'all' as ca ,'all' as ac,count(1) as ct  from tmp group by pl,dt union all
select 'all' as pl,dt, 'all' as ca ,'all' as ac,count(1) as ct  from tmp group by dt
) as tmp2
insert overwrite into stats_event
select date_convert(dt),platform_convert(pl),event_convert(ca,ac),sum(ct),dt
group by pl,dt,ca,ac
;

echo "run sqoop statment..."
sqoop export --connect jdbc:mysql://hdp01:3306/test \
--username hive --password 123456 --table stats_event \
--export-dir 'hdfs://192.168.152.10:9000/user/hive/warehouse/gp1809.db/stats_event/*' \
--input-fields-terminated-by '\001' \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,event_dimension_id
;
echo "event job is finished..."







