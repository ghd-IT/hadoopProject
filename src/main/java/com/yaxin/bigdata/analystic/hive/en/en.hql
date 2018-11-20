1、创建事件的基础维度类，不需要使用集合维度类对象

2、创建获取维度id的udf函数,并测试
create function date_convert as 'com.yaxin.bigdata.analystic.hive.DateDimensionUdf' using jar "hdfs://hdp01/logs/utfjars/hadoopProject-1.0-SNAPSHOT.jar"

3、创建hive表映射每一天的数据，创建分区表

4、创建最终结果表
CREATE external TABLE `stats_event` (
  `platform_dimension_id` int ,
  `date_dimension_id` int,
  `event_dimension_id` int,
  `times` int,
  `created` string
);

5、写 hql
with tmp as(
select de.s_time,de.pl,de.ca,de.ac,count(1)
from loges de
where pl is not null
and de.month =${}
and de.dat=${}
group by de.s_time,de.pl,de.ca,de.ac
)
from(select all ......from tmp union all
select ...... union all
select ...... union all
select ...... union all
select ...... )
insert into stats_event
select  date_covert(dt),platform_convert(pl),
group by ;

6、扩展维度，并将结果集导出到结果表中 union all

7、使用sqoop体育局将结果表导出到mysql

8、将整个封装成shell脚本
判断时间，没有默认昨天的时间执行

hive --database gp1809 -e "";







