create table order_info(
order_id string,
platform string,
s_time string,
currency_type string,
payment_type string,
amount int
);

insert overwrite table order_info
select distinct * from (
select oid,pl,from_unixtime(cast(s_time/1000 as bigint),"yyyy-MM-dd"),cut,pt,sum(cua)
from logs
where pl is not null
and oid!="null"
and month="11" and day ="02"
group by oid,pl,s_time,cut,pt
) tmp



sqoop export --connect jdbc:mysql://hdp01:3306/test \
--username hive --password 123456 --table order_info \
--export-dir 'hdfs://192.168.152.10:9000/user/hive/warehouse/gp1809.db/order_info/*' \
--input-fields-terminated-by '\001' \
--update-mode allowinsert






CREATE TABLE `stats_order` (
  `platform_dimension_id` int,
  `date_dimension_id` int ,
  `currency_type_dimension_id` int,
  `payment_type_dimension_id` int,
  `orders` int,
  `success_orders` int,
  `refund_orders` int,
  `order_amount` int,
  `revenue_amount` int,
  `refund_amount` int,
  `total_revenue_amount` int,
  `total_refund_amount` int,
  `created` date
);


CREATE TABLE `stats_order` (
  `platform_dimension_id` int(11) NOT NULL DEFAULT '0',
  `date_dimension_id` int(11) NOT NULL DEFAULT '0',
  `currency_type_dimension_id` int(11) NOT NULL DEFAULT '0',
  `payment_type_dimension_id` int(11) NOT NULL DEFAULT '0',
  `orders` int(11) DEFAULT '0' COMMENT '订单个数',
  `success_orders` int(11) DEFAULT '0' COMMENT '成功支付的订单个数',
  `refund_orders` int(11) DEFAULT '0' COMMENT '退款订单个数',
  `order_amount` int(11) DEFAULT '0' COMMENT '订单金额',
  `revenue_amount` int(11) DEFAULT '0' COMMENT '收入金额，也就是成功支付过的金额',
  `refund_amount` int(11) DEFAULT '0' COMMENT '退款金额',
  `total_revenue_amount` int(11) DEFAULT '0' COMMENT '迄今为止，总的订单交易额',
  `total_refund_amount` int(11) DEFAULT '0' COMMENT '迄今为止，总的退款金额',
  `created` date DEFAULT NULL,
  PRIMARY KEY (`platform_dimension_id`,`date_dimension_id`,`currency_type_dimension_id`,`payment_type_dimension_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='统计订单信息的统计表';

create table dw_or(
pl string,
s_time string,
en string,
oid string,
cut string,
pt string,
cua int
);

insert into table dw_or
select pl,s_time,en,oid,cut,pt,cua
from logs
where en="e_cs" or en ="e_cr" or en="e_crt"


insert into table stats_order
select  b.pl,b.da,b.cut,b.pt,b.oid ,b.cs_en,b.cr_en,b.cua,(case when b.cs_cua is null then 0 else b.cs_cua end),(case when b.cr_cua is null then 0 else b.cr_cua end),sum(case when b.cs_cua is null then 0 else b.cs_cua end) over(distribute by  b.pl sort by b.da),sum(case when b.cr_cua is null then 0 else b.cr_cua end) over(distribute by  b.pl sort by b.da), b.dt
from(
select platform_convert(pl) pl,date_convert(dt) da,currency_type_convert(cut) cut,payment_type_convert(pt) pt,count(oid) oid ,count(cs_en) cs_en ,count(cr_en) cr_en ,sum(cua) cua,sum(cs_cua) cs_cua,sum(cr_cua) cr_cua , dt
from (select pl as pl,from_unixtime(cast(s_time/1000 as bigint),"yyyy-MM-dd") dt,cut as cut,pt as pt,oid as oid ,(case when en="e_cs" then en end) as cs_en  ,(case when en="e_cr" then en end) as cr_en,cua as cua ,(case when en="e_cs" then cua end) as cs_cua ,(case when en="e_cr" then cua end) as cr_cua
from dw_or dd) as a
group by pl,dt,cut,pt ) as b



sqoop export --connect jdbc:mysql://hdp01:3306/test \
--username hive --password 123456 --table stats_order \
--export-dir 'hdfs://192.168.152.10:9000/user/hive/warehouse/gp1809.db/stats_order/*' \
--input-fields-terminated-by '\001' \
--update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id
