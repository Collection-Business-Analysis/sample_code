
-------------------------资产明细表关联C卡分----------------------------



drop table IF EXISTS dm_aifox.jf_asset_detail_jf FORCE;
create table dm_aifox.jf_asset_detail as 

with jf_asset_detail as (
select a.asset_item_no,
a.user_id,
if(a.user_debt_status='new_user','new','old') AS user_status,
a.user_debt_status,
a.apply_channel_source,
c.asset_loan_channel,
c.product_form,
c.product_period_unit,
concat(c.product_period_qty,'天','*',c.product_period_cnt,'期') as product,
date_format(a.grant_time,'%Y-%m-%d') AS grant_time,
datediff(a.finish_time,a.delay_due_time) AS date_diff,
datediff(if(a.finish_time is null,curdate(),a.finish_time),a.delay_due_time) AS date_cur_diff,
date_format(a.delay_due_time,'%Y-%m') AS delay_due_month,
a.period_seq,
date(date_add(date(a.delay_due_time), INTERVAL  if(DAYOFWEEK(a.delay_due_time) in(5,6,7),-DAYOFWEEK(a.delay_due_time)+5, -2 -DAYOFWEEK(a.delay_due_time)) day )) as due_week_star,
date_format(a.delay_due_time,'%Y-%m-%d') AS delay_due_time,
date_format(a.finish_time,'%Y-%m-%d') AS finish_dt,
a.finish_time,
a.overdue_period_days,
a.if_api,
d.asset_extend_ref_order_type,
e.memo,
case when e.asset_order_type ='Q' and asset_source ='APP' then e.memo 
      when e.asset_order_type ='Q' and asset_source ='API' and asset_charge_type ='IRR36' then e.memo 
			when e.asset_order_type ='Q' and asset_source ='API' and asset_charge_type ='APR36' then 'APR-API资产' 
else '双融单' end as log_type,
e.asset_charge_type,
case when a.granted_principal_period_amt<=500 then '1(0,500]' 
     when a.granted_principal_period_amt<=1200 then '2(500,1200]' 
     else '3(1200+)' end as amt_type_1,
case when a.granted_principal_period_amt<=100 then '(0,100]'
    when a.granted_principal_period_amt<=200 then '(100,200]'
    when a.granted_principal_period_amt<=300 then '(200,300]'
    when a.granted_principal_period_amt<=400 then '(300,400]'
    when a.granted_principal_period_amt<=500 then '(400,500]'
    when a.granted_principal_period_amt<=600 then '(500,600]'
    when a.granted_principal_period_amt<=700 then '(600,700]'
    when a.granted_principal_period_amt<=800 then '(700,800]'
    when a.granted_principal_period_amt<=900 then '(800,900]'
    when a.granted_principal_period_amt<=1000 then '(900,1000]'
else '(1000+)' end as amt_type_2,
a.granted_principal_period_amt,
a.repaid_principal_period_amt,
if(a.overdue_period_days IN (0, 1) ,'是','否' ) as is_new_overdue,
if(a.asset_overdue_period_days IN (0, 1) ,'是','否' ) as is_asset_new_overdue
FROM dwb.dwb_asset_period_info AS a
INNER JOIN dwb.dwb_asset_info AS c 
ON a.asset_item_no = c.asset_item_no
left join   dwd.dwd_asset_main as d 
on a.asset_item_no =d.asset_item_no
left join dim.dim_asset_feature  as e 
on d.asset_extend_ref_order_type=e.source_type
WHERE 1=1
and a.grant_time >= '2023-01-01'
AND date_format(a.delay_due_time,'%Y-%m-01') >= '${dt_s}'
AND date(a.delay_due_time) <= date_add(curdate(), INTERVAL 10 DAY)
)


, d3level as (
  select
   date(etl_time) as dt,
    asset_item_no,
    asset_overdue_days as asset_due_days,
    asset_period,
    max(cscore_level) as max_D3_level,
    max(cscore) as max_D3_score
  from hive.dm_feature.dm_cscore_all_model_daily_v6
  where substr(etl_time,1,10) >= '${dt_s}'
  and asset_overdue_days = 3
  group by 1,2,3,4
  )


, d1level as (
  select
   date(etl_time) as dt,
    asset_item_no,
    asset_overdue_days as asset_due_days,
    asset_period,
    max(cscore_level) as max_D1_level,
    max(cscore) as max_D1_score
  from hive.dm_feature.dm_cscore_all_model_daily_v6
  where substr(etl_time,1,10) >= '${dt_s}'
  and asset_overdue_days = 1
  group by 1,2,3,4
  )
	
, d0level as (
  select
   date(etl_time) as dt,
    asset_item_no,
    asset_overdue_days,
    asset_period,
    max(cscore_level) as max_D0_level,
    max(cscore) as max_D0_score
  from hive.dm_feature.dm_cscore_all_model_daily_v6
  where substr(etl_time,1,10) >= '${dt_s}'
  and asset_overdue_days = 0
  group by 1,2,3,4
  )
, d_5level as (
  select
   date(etl_time) as dt,
    asset_item_no,
    asset_overdue_days as asset_due_days,
    asset_period,
    max(cscore_level) as max_D_5_level,
    max(cscore) as max_D_5_score
  from hive.dm_feature.dm_cscore_all_model_daily_v6
  where substr(etl_time,1,10) >= '${dt_s}'
  and asset_overdue_days = -5
  group by 1,2,3,4
  )
-- , d_3level as (
--   select
--    date(etl_time) as dt,
--     asset_item_no,
--     asset_overdue_days as asset_due_days,
--     asset_period,
--     max(cscore_level) as max_D_3_level,
--     max(cscore) as max_D_3_score
--   from hive.dm_feature.dm_cscore_all_model_daily_v6
--   where substr(etl_time,1,10) >= '${dt_s}'
--   and asset_overdue_days = -3
--   group by 1,2,3,4
--   )
, d_1level as (
  select
   date(etl_time) as dt,
    asset_item_no,
    asset_overdue_days as asset_due_days,
    asset_period,
    max(cscore_level) as max_D_1_level,
    max(cscore) as max_D_1_score
  from hive.dm_feature.dm_cscore_all_model_daily_v6
  where substr(etl_time,1,10) >= '${dt_s}'
  and asset_overdue_days = -1
  group by 1,2,3,4
  )	
	
,qy_rte_01 as 
(select 
asset_item_no
,asset_extend_ref_order_type
,asset_create_at
,asset_sign_at
,JSON_UNQUOTE(JSON_EXTRACT(asset_extend_info, '$.post_order_List[0].order_no')) AS first_order_no
,JSON_EXTRACT(asset_extend_info, '$.post_order_List[0].value') AS first_value
,JSON_UNQUOTE(JSON_EXTRACT(asset_extend_info, '$.post_order_List[1].order_no')) AS se_order_no
,JSON_EXTRACT(asset_extend_info, '$.post_order_List[1].value') AS se_value
from     dwd.dwd_asset_main 
where 1=1
and asset_extend_ref_order_type ='apr36_quanyi_liexiong')

,qy_rte_02 as (
select 
a.asset_item_no
,a.asset_extend_ref_order_type
,a.asset_create_at
,a.asset_sign_at
,a.first_order_no
,b.ref_order_type
,b.ref_order_name
,a.first_value
,case when b.ref_order_type ='liexiong_minor' then first_value else round(1- first_value,1) end as qy_value
from qy_rte_01 as a 
left join (select * from dws.dws_asset_all_period_info where period_seq =1 )as b 
on a.first_order_no =b.asset_item_no
where a.asset_sign_at<>'1000-01-01 00:00:00'
)
,day_detial as(
select
a.user_id,
a.asset_item_no,
a.user_status,
a.user_debt_status,
date_format(a.grant_time,'%Y-%m') AS grant_mth,
a.grant_time,
a.delay_due_time,
date_add(a.delay_due_time, INTERVAL -1 DAY) as d_1_delay_due_dt,
day(a.delay_due_time) as due_day,
a.due_week_star,
a.delay_due_month,
a.finish_time,
a.finish_dt,
a.overdue_period_days,
a.date_diff,
a.date_cur_diff,
a.amt_type_1,
a.amt_type_2,
a.granted_principal_period_amt,
a.repaid_principal_period_amt,
a.product,
a.period_seq,
a.if_api,
a.product_form,
a.apply_channel_source,
a.asset_loan_channel,
a.is_new_overdue,
a.is_asset_new_overdue,
g.max_D_5_level,
-- d.max_D_3_level,
b.max_D0_level,
c.max_D1_level,
e.max_D_1_level,
cast(g.max_D_5_score as float) as max_D_5_score ,
-- cast(d.max_D_3_score as float) as max_D_3_score ,
cast(b.max_D0_score as float) as max_D0_score,
cast(c.max_D1_score as float) as max_D1_score,
cast(e.max_D_1_score as float) as max_D_1_score,
a.log_type,
f.qy_value,
if(date(a.delay_due_time) < curdate(), a.granted_principal_period_amt, 0) AS due_cnt,
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 7 DAY) AND (date_diff IS NULL OR date_diff > -7), a.granted_principal_period_amt, 0) AS D_7_overdue,
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 6 DAY) AND (date_diff IS NULL OR date_diff > -6), a.granted_principal_period_amt, 0) AS D_6_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 5 DAY) AND (date_diff IS NULL OR date_diff > -5), a.granted_principal_period_amt, 0) AS D_5_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 4 DAY) AND (date_diff IS NULL OR date_diff > -4), a.granted_principal_period_amt, 0) AS D_4_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 3 DAY) AND (date_diff IS NULL OR date_diff > -3), a.granted_principal_period_amt, 0) AS D_3_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 2 DAY) AND (date_diff IS NULL OR date_diff > -2), a.granted_principal_period_amt, 0) AS D_2_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 1 DAY) AND (date_diff IS NULL OR date_diff > -1), a.granted_principal_period_amt, 0) AS D_1_overdue, 
if(date(a.delay_due_time) < curdate() AND (date_diff IS NULL OR date_diff > 0), a.granted_principal_period_amt, 0) AS D0_overdue,
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -1 DAY) AND (date_diff IS NULL OR date_diff > 1), a.granted_principal_period_amt, 0) AS D1_overdue,
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -2 DAY) AND (date_diff IS NULL OR date_diff > 2), a.granted_principal_period_amt, 0) AS D2_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff > 3), a.granted_principal_period_amt, 0) AS D3_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -4 DAY) AND (date_diff IS NULL OR date_diff > 4), a.granted_principal_period_amt, 0) AS D4_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -5 DAY) AND (date_diff IS NULL OR date_diff > 5), a.granted_principal_period_amt, 0) AS D5_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -6 DAY) AND (date_diff IS NULL OR date_diff > 6), a.granted_principal_period_amt, 0) AS D6_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff > 7), a.granted_principal_period_amt, 0) AS D7_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff > 15), a.granted_principal_period_amt, 0) AS D15_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff > 30), a.granted_principal_period_amt, 0) AS D30_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff > 60), a.granted_principal_period_amt, 0) AS D60_overdue, 
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff > 90), a.granted_principal_period_amt, 0) AS D90_overdue 
from asset_detail as a
left join d0level as b
on a.asset_item_no = b.asset_item_no
and a.period_seq = b.asset_period
and a.delay_due_time=b.dt
left join d1level as c
on a.asset_item_no = c.asset_item_no
and a.period_seq = c.asset_period
and date_add(a.delay_due_time, INTERVAL 1 DAY)=c.dt
-- left join d_3level as d
-- on a.asset_item_no = d.asset_item_no
-- and a.period_seq = d.asset_period
-- and date_add(a.delay_due_time, INTERVAL -3 DAY)=d.dt
left join d_1level as e
on a.asset_item_no = e.asset_item_no
and a.period_seq = e.asset_period
and date_add(a.delay_due_time, INTERVAL -1 DAY)=e.dt
left join qy_rte_02 as f 
on  a.asset_item_no =f.asset_item_no
left join d_3level as g
on a.asset_item_no = g.asset_item_no
and a.period_seq = g.asset_period
and date_add(a.delay_due_time, INTERVAL -5 DAY)=g.dt

where 1=1
)

select * from day_detial







---------------------------资产明细表微聚合--------------------------

drop table IF EXISTS dm_aifox.jf_asset_group FORCE;
create table dm_aifox.jf_asset_group as 
select
  delay_due_month,
  due_week_star,
  delay_due_time,
  due_day,
  if_api,
  user_debt_status,
  product,
  period_seq,
  apply_channel_source,
  amt_type_1,
  is_new_overdue,
  is_asset_new_overdue,
  log_type,
  qy_value,
  -- C_level相关字段
  max_D_5_level,
--   max_D_3_level,
  max_D_1_level,
  max_D0_level,
  max_D1_level,
  CASE WHEN D_6_overdue > 0 THEN 1 ELSE 0 END AS is_D_5_gt0,
--   CASE WHEN D_4_overdue > 0 THEN 1 ELSE 0 END AS is_D_3_gt0,
  CASE WHEN D_2_overdue > 0 THEN 1 ELSE 0 END AS is_D_1_gt0,
  CASE WHEN D_1_overdue > 0 THEN 1 ELSE 0 END AS is_D0_gt0,
  CASE WHEN D0_overdue > 0 THEN 1 ELSE 0 END AS is_D1_gt0,
  sum(cast(max_D_5_score as float)) AS sum_D_5_score,
--   sum(cast(max_D_3_score as float)) AS sum_D_3_score,
  sum(cast(max_D_1_score as float)) AS sum_D_1_score,
  sum(cast(max_D0_score as float)) AS sum_D0_score,
  sum(cast(max_D1_score as float)) AS sum_D1_score,
  sum(due_cnt) AS due_cnt,
  sum(D_7_overdue) AS D_7_overdue,
  sum(D_6_overdue) AS D_6_overdue,
  sum(D_5_overdue) AS D_5_overdue,
  sum(D_4_overdue) AS D_4_overdue,
  sum(D_3_overdue) AS D_3_overdue,
  sum(D_2_overdue) AS D_2_overdue,
  sum(D_1_overdue) AS D_1_overdue,
  sum(D0_overdue) AS D0_overdue,
  sum(D1_overdue) AS D1_overdue,
  sum(D2_overdue) AS D2_overdue,
  sum(D3_overdue) AS D3_overdue,
  sum(D4_overdue) AS D4_overdue,
  sum(D5_overdue) AS D5_overdue,
  sum(D6_overdue) AS D6_overdue,
  sum(D7_overdue) AS D7_overdue,
  sum(D15_overdue) AS D15_overdue,
  sum(D30_overdue) AS D30_overdue,
  sum(D60_overdue) AS D60_overdue,
  sum(D90_overdue) AS D90_overdue,
  count(distinct case when date(delay_due_time) < curdate() then asset_item_no end ) AS log_due_cnt,
  count(distinct case when D_7_overdue>0 then asset_item_no end) AS log_D_7_overdue,
  count(distinct case when D_6_overdue>0 then asset_item_no end) AS log_D_6_overdue,
  count(distinct case when D_5_overdue>0 then asset_item_no end) AS log_D_5_overdue,
  count(distinct case when D_4_overdue>0 then asset_item_no end) AS log_D_4_overdue,
  count(distinct case when D_3_overdue>0 then asset_item_no end) AS log_D_3_overdue,
  count(distinct case when D_2_overdue>0 then asset_item_no end) AS log_D_2_overdue,
  count(distinct case when D_1_overdue>0 then asset_item_no end) AS log_D_1_overdue,
  count(distinct case when D0_overdue>0 then asset_item_no end ) AS log_D0_overdue,
  count(distinct case when D1_overdue>0 then asset_item_no end) AS log_D1_overdue,
  count(distinct case when D2_overdue>0 then asset_item_no end) AS log_D2_overdue,
  count(distinct case when D3_overdue>0 then asset_item_no end) AS log_D3_overdue,
  count(distinct case when D4_overdue>0 then asset_item_no end) AS log_D4_overdue,
  count(distinct case when D5_overdue>0 then asset_item_no end) AS log_D5_overdue,
  count(distinct case when D6_overdue>0 then asset_item_no end) AS log_D6_overdue,
  count(distinct case when D7_overdue>0 then asset_item_no end) AS log_D7_overdue,
  count(distinct case when D15_overdue>0 then asset_item_no end) AS log_D15_overdue,
  count(distinct case when D30_overdue>0 then asset_item_no end) AS log_D30_overdue,
  count(distinct case when D60_overdue>0 then asset_item_no end) AS log_D60_overdue,
  count(distinct case when D90_overdue>0 then asset_item_no end) AS log_D90_overdue
from (
  select * from dm_aifox.jf_asset_detail 
  where delay_due_time >= '2025-01-01'
) t  -- 给子查询添加别名 t
group by  -- 确保分组字段序号与select中非聚合字段顺序一致
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22;





---------------------------核心中间表--------------------------




drop table  IF EXISTS dm_aifox.jf_cn_asset_c_card FORCE ;

create   table  dm_aifox.jf_cn_asset_c_card   as 

 select
    "金额" as stat_type,
    'D-5' as overdue_type,
    delay_due_month,
    due_week_star,
    cast(delay_due_time as date) as delay_due_time,
    due_day,
    if_api,
    user_debt_status,
    product,
    period_seq,
    apply_channel_source,
    amt_type_1,
		log_type,
        is_new_overdue,
        is_asset_new_overdue,
    max_D_5_level as C_level,
    sum(coalesce(sum_D_5_score,0)) as C_score,
    sum(due_cnt) AS due_cnt,
    sum(D_7_overdue) AS D_7_overdue,
    sum(D_6_overdue) AS D_6_overdue,
    sum(D_5_overdue) AS D_5_overdue,
    sum(D_4_overdue) AS D_4_overdue,
    sum(D_3_overdue) AS D_3_overdue,
    sum(D_2_overdue) AS D_2_overdue,
    sum(D_1_overdue) AS D_1_overdue,
    sum(D0_overdue) AS D0_overdue,
    sum(D1_overdue) AS D1_overdue,
    sum(D2_overdue) AS D2_overdue,
    sum(D3_overdue) AS D3_overdue,
    sum(D4_overdue) AS D4_overdue,
    sum(D5_overdue) AS D5_overdue,
    sum(D6_overdue) AS D6_overdue,
    sum(D7_overdue) AS D7_overdue,
    sum(D15_overdue) AS D15_overdue,
    sum(D30_overdue) AS D30_overdue,
    sum(D60_overdue) AS D60_overdue,
    sum(D90_overdue) AS D90_overdue
from dm_aifox.jf_asset_group
where is_D_5_gt0=1
-- and is_new_overdue='是'
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16
-- limit 10		
		
union all
select
    "件数" as stat_type,
    'D-5' as overdue_type,
    delay_due_month,
    due_week_star,
    cast(delay_due_time as date) as delay_due_time,
    due_day,
    if_api,
    user_debt_status,
    product,
    period_seq,
    apply_channel_source,
    amt_type_1,
    log_type,
     is_new_overdue,
        is_asset_new_overdue,
    max_D_5_level as C_level,
    sum(coalesce(sum_D_5_score,0)) as C_score,
    sum(log_due_cnt) AS due_cnt,
    sum(log_D_7_overdue) AS D_7_overdue,
    sum(log_D_6_overdue) AS D_6_overdue,
    sum(log_D_5_overdue) AS D_5_overdue,
    sum(log_D_4_overdue) AS D_4_overdue,
    sum(log_D_3_overdue) AS D_3_overdue,
    sum(log_D_2_overdue) AS D_2_overdue,
    sum(log_D_1_overdue) AS D_1_overdue,
    sum(log_D0_overdue) AS D0_overdue,
    sum(log_D1_overdue) AS D1_overdue,
    sum(log_D2_overdue) AS D2_overdue,
    sum(log_D3_overdue) AS D3_overdue,
    sum(log_D4_overdue) AS D4_overdue,
    sum(log_D5_overdue) AS D5_overdue,
    sum(log_D6_overdue) AS D6_overdue,
    sum(log_D7_overdue) AS D7_overdue,
    sum(log_D15_overdue) AS D15_overdue,
    sum(log_D30_overdue) AS D30_overdue,
    sum(log_D60_overdue) AS D60_overdue,
    sum(log_D90_overdue) AS D90_overdue
from dm_aifox.jf_asset_group
where is_D_5_gt0=1
-- and is_new_overdue='是'
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16
		
-- union all
--  select
--     "金额" as stat_type,
--     'D-3' as overdue_type,
--     delay_due_month,
--     due_week_star,
--     cast(delay_due_time as date) as delay_due_time,
--     due_day,
--     if_api,
--     user_debt_status,
--     product,
--     period_seq,
--     apply_channel_source,
--     amt_type_1,
-- 		log_type,
--         is_new_overdue,
--         is_asset_new_overdue,
--     max_D_3_level as C_level,
--     sum(coalesce(sum_D_3_score,0)) as C_score,
--     sum(due_cnt) AS due_cnt,
--     sum(D_7_overdue) AS D_7_overdue,
--     sum(D_6_overdue) AS D_6_overdue,
--     sum(D_5_overdue) AS D_5_overdue,
--     sum(D_4_overdue) AS D_4_overdue,
--     sum(D_3_overdue) AS D_3_overdue,
--     sum(D_2_overdue) AS D_2_overdue,
--     sum(D_1_overdue) AS D_1_overdue,
--     sum(D0_overdue) AS D0_overdue,
--     sum(D1_overdue) AS D1_overdue,
--     sum(D2_overdue) AS D2_overdue,
--     sum(D3_overdue) AS D3_overdue,
--     sum(D4_overdue) AS D4_overdue,
--     sum(D5_overdue) AS D5_overdue,
--     sum(D6_overdue) AS D6_overdue,
--     sum(D7_overdue) AS D7_overdue,
--     sum(D15_overdue) AS D15_overdue,
--     sum(D30_overdue) AS D30_overdue,
--     sum(D60_overdue) AS D60_overdue,
--     sum(D90_overdue) AS D90_overdue
-- from dm_aifox.jf_asset_group
-- where is_D_3_gt0=1
-- -- and is_new_overdue='是'
-- group by
--     1,
--     2,
--     3,
--     4,
--     5,
--     6,
--     7,
--     8,
--     9,
--     10,
--     11,
--     12,
--     13,
--     14,
--     15,
--     16
-- -- limit 10		
		
-- union all
-- select
--     "件数" as stat_type,
--     'D-3' as overdue_type,
--     delay_due_month,
--     due_week_star,
--     cast(delay_due_time as date) as delay_due_time,
--     due_day,
--     if_api,
--     user_debt_status,
--     product,
--     period_seq,
--     apply_channel_source,
--     amt_type_1,
--     log_type,
--      is_new_overdue,
--         is_asset_new_overdue,
--     max_D_3_level as C_level,
--     sum(coalesce(sum_D_3_score,0)) as C_score,
--     sum(log_due_cnt) AS due_cnt,
--     sum(log_D_7_overdue) AS D_7_overdue,
--     sum(log_D_6_overdue) AS D_6_overdue,
--     sum(log_D_5_overdue) AS D_5_overdue,
--     sum(log_D_4_overdue) AS D_4_overdue,
--     sum(log_D_3_overdue) AS D_3_overdue,
--     sum(log_D_2_overdue) AS D_2_overdue,
--     sum(log_D_1_overdue) AS D_1_overdue,
--     sum(log_D0_overdue) AS D0_overdue,
--     sum(log_D1_overdue) AS D1_overdue,
--     sum(log_D2_overdue) AS D2_overdue,
--     sum(log_D3_overdue) AS D3_overdue,
--     sum(log_D4_overdue) AS D4_overdue,
--     sum(log_D5_overdue) AS D5_overdue,
--     sum(log_D6_overdue) AS D6_overdue,
--     sum(log_D7_overdue) AS D7_overdue,
--     sum(log_D15_overdue) AS D15_overdue,
--     sum(log_D30_overdue) AS D30_overdue,
--     sum(log_D60_overdue) AS D60_overdue,
--     sum(log_D90_overdue) AS D90_overdue
-- from dm_aifox.jf_asset_group
-- where is_D_3_gt0=1
-- -- and is_new_overdue='是'
-- group by
--     1,
--     2,
--     3,
--     4,
--     5,
--     6,
--     7,
--     8,
--     9,
--     10,
--     11,
--     12,
--     13,
--     14,
--     15,
--     16
		
union all
select
    "金额" as stat_type,
    'D_1' as overdue_type,
    delay_due_month,
    due_week_star,
     cast(delay_due_time as date) as delay_due_time,
    due_day,
    if_api,
    user_debt_status,
    product,
    period_seq,
    apply_channel_source,
    amt_type_1,
    log_type,
     is_new_overdue,
        is_asset_new_overdue,
    max_D_1_level as C_level,
    sum(coalesce(sum_D_1_score,0)) as C_score,
    sum(due_cnt) AS due_cnt,
    sum(D_7_overdue) AS D_7_overdue,
    sum(D_6_overdue) AS D_6_overdue,
    sum(D_5_overdue) AS D_5_overdue,
    sum(D_4_overdue) AS D_4_overdue,
    sum(D_3_overdue) AS D_3_overdue,
    sum(D_2_overdue) AS D_2_overdue,
    sum(D_1_overdue) AS D_1_overdue,
    sum(D0_overdue) AS D0_overdue,
    sum(D1_overdue) AS D1_overdue,
    sum(D2_overdue) AS D2_overdue,
    sum(D3_overdue) AS D3_overdue,
    sum(D4_overdue) AS D4_overdue,
    sum(D5_overdue) AS D5_overdue,
    sum(D6_overdue) AS D6_overdue,
    sum(D7_overdue) AS D7_overdue,
    sum(D15_overdue) AS D15_overdue,
    sum(D30_overdue) AS D30_overdue,
    sum(D60_overdue) AS D60_overdue,
    sum(D90_overdue) AS D90_overdue
from dm_aifox.jf_asset_group
where is_D_1_gt0=1
-- and is_new_overdue='是'
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16
union all
select
    "件数" as stat_type,
    'D_1' as overdue_type,
    delay_due_month,
    due_week_star,
     cast(delay_due_time as date) as delay_due_time,
    due_day,
    if_api,
    user_debt_status,
    product,
    period_seq,
    apply_channel_source,
    amt_type_1,
    log_type,
     is_new_overdue,
        is_asset_new_overdue,
    max_D_1_level as C_level,
    sum(coalesce(sum_D_1_score,0)) as C_score,
    sum(log_due_cnt) AS due_cnt,
    sum(log_D_7_overdue) AS D_7_overdue,
    sum(log_D_6_overdue) AS D_6_overdue,
    sum(log_D_5_overdue) AS D_5_overdue,
    sum(log_D_4_overdue) AS D_4_overdue,
    sum(log_D_3_overdue) AS D_3_overdue,
    sum(log_D_2_overdue) AS D_2_overdue,
    sum(log_D_1_overdue) AS D_1_overdue,
    sum(log_D0_overdue) AS D0_overdue,
    sum(log_D1_overdue) AS D1_overdue,
    sum(log_D2_overdue) AS D2_overdue,
    sum(log_D3_overdue) AS D3_overdue,
    sum(log_D4_overdue) AS D4_overdue,
    sum(log_D5_overdue) AS D5_overdue,
    sum(log_D6_overdue) AS D6_overdue,
    sum(log_D7_overdue) AS D7_overdue,
    sum(log_D15_overdue) AS D15_overdue,
    sum(log_D30_overdue) AS D30_overdue,
    sum(log_D60_overdue) AS D60_overdue,
    sum(log_D90_overdue) AS D90_overdue
from dm_aifox.jf_asset_group
where is_D_1_gt0=1
-- and is_new_overdue='是'
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16

union all
select
    "金额" as stat_type,
    'D0' as overdue_type,
    delay_due_month,
    due_week_star,
     cast(delay_due_time as date) as delay_due_time,
    due_day,
    if_api,
    user_debt_status,
    product,
    period_seq,
    apply_channel_source,
    amt_type_1,
    log_type,
     is_new_overdue,
        is_asset_new_overdue,
    max_D0_level as C_level,
    sum(coalesce(sum_D0_score,0)) as C_score,
    sum(due_cnt) AS due_cnt,
    sum(D_7_overdue) AS D_7_overdue,
    sum(D_6_overdue) AS D_6_overdue,
    sum(D_5_overdue) AS D_5_overdue,
    sum(D_4_overdue) AS D_4_overdue,
    sum(D_3_overdue) AS D_3_overdue,
    sum(D_2_overdue) AS D_2_overdue,
    sum(D_1_overdue) AS D_1_overdue,
    sum(D0_overdue) AS D0_overdue,
    sum(D1_overdue) AS D1_overdue,
    sum(D2_overdue) AS D2_overdue,
    sum(D3_overdue) AS D3_overdue,
    sum(D4_overdue) AS D4_overdue,
    sum(D5_overdue) AS D5_overdue,
    sum(D6_overdue) AS D6_overdue,
    sum(D7_overdue) AS D7_overdue,
    sum(D15_overdue) AS D15_overdue,
    sum(D30_overdue) AS D30_overdue,
    sum(D60_overdue) AS D60_overdue,
    sum(D90_overdue) AS D90_overdue
from dm_aifox.jf_asset_group
where is_D0_gt0=1
-- and is_new_overdue='是'
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16
union all
select
    "件数" as stat_type,
    'D0' as overdue_type,
    delay_due_month,
    due_week_star,
     cast(delay_due_time as date) as delay_due_time,
    due_day,
    if_api,
    user_debt_status,
    product,
    period_seq,
    apply_channel_source,
    amt_type_1,
    log_type,
     is_new_overdue,
        is_asset_new_overdue,
    max_D0_level as C_level,
    sum(coalesce(sum_D0_score,0)) as C_score,
    sum(log_due_cnt) AS due_cnt,
    sum(log_D_7_overdue) AS D_7_overdue,
    sum(log_D_6_overdue) AS D_6_overdue,
    sum(log_D_5_overdue) AS D_5_overdue,
    sum(log_D_4_overdue) AS D_4_overdue,
    sum(log_D_3_overdue) AS D_3_overdue,
    sum(log_D_2_overdue) AS D_2_overdue,
    sum(log_D_1_overdue) AS D_1_overdue,
    sum(log_D0_overdue) AS D0_overdue,
    sum(log_D1_overdue) AS D1_overdue,
    sum(log_D2_overdue) AS D2_overdue,
    sum(log_D3_overdue) AS D3_overdue,
    sum(log_D4_overdue) AS D4_overdue,
    sum(log_D5_overdue) AS D5_overdue,
    sum(log_D6_overdue) AS D6_overdue,
    sum(log_D7_overdue) AS D7_overdue,
    sum(log_D15_overdue) AS D15_overdue,
    sum(log_D30_overdue) AS D30_overdue,
    sum(log_D60_overdue) AS D60_overdue,
    sum(log_D90_overdue) AS D90_overdue
from dm_aifox.jf_asset_group
where is_D0_gt0=1
-- and is_new_overdue='是'
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16
union all
select
    "金额" as stat_type,
    'D1' as overdue_type,
    delay_due_month,
    due_week_star,
     cast(delay_due_time as date) as delay_due_time,
    due_day,
    if_api,
    user_debt_status,
    product,
    period_seq,
    apply_channel_source,
    amt_type_1,
    log_type,
     is_new_overdue,
        is_asset_new_overdue,
    max_D1_level as C_level,
    sum(coalesce(sum_D1_score,0)) as C_score,
    sum(due_cnt) AS due_cnt,
    sum(D_7_overdue) AS D_7_overdue,
    sum(D_6_overdue) AS D_6_overdue,
    sum(D_5_overdue) AS D_5_overdue,
    sum(D_4_overdue) AS D_4_overdue,
    sum(D_3_overdue) AS D_3_overdue,
    sum(D_2_overdue) AS D_2_overdue,
    sum(D_1_overdue) AS D_1_overdue,
    sum(D0_overdue) AS D0_overdue,
    sum(D1_overdue) AS D1_overdue,
    sum(D2_overdue) AS D2_overdue,
    sum(D3_overdue) AS D3_overdue,
    sum(D4_overdue) AS D4_overdue,
    sum(D5_overdue) AS D5_overdue,
    sum(D6_overdue) AS D6_overdue,
    sum(D7_overdue) AS D7_overdue,
    sum(D15_overdue) AS D15_overdue,
    sum(D30_overdue) AS D30_overdue,
    sum(D60_overdue) AS D60_overdue,
    sum(D90_overdue) AS D90_overdue
from dm_aifox.jf_asset_group
where is_D1_gt0=1
-- and is_new_overdue='是'
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16
union all
select
    "件数" as stat_type,
    'D1' as overdue_type,
    delay_due_month,
    due_week_star,
     cast(delay_due_time as date) as delay_due_time,
    due_day,
    if_api,
    user_debt_status,
    product,
    period_seq,
    apply_channel_source,
    amt_type_1,
    log_type,
    is_new_overdue,
        is_asset_new_overdue,
    max_D1_level as C_level,
    sum(coalesce(sum_D1_score,0)) as C_score,
    sum(log_due_cnt) AS due_cnt,
    sum(log_D_7_overdue) AS D_7_overdue,
    sum(log_D_6_overdue) AS D_6_overdue,
    sum(log_D_5_overdue) AS D_5_overdue,
    sum(log_D_4_overdue) AS D_4_overdue,
    sum(log_D_3_overdue) AS D_3_overdue,
    sum(log_D_2_overdue) AS D_2_overdue,
    sum(log_D_1_overdue) AS D_1_overdue,
    sum(log_D0_overdue) AS D0_overdue,
    sum(log_D1_overdue) AS D1_overdue,
    sum(log_D2_overdue) AS D2_overdue,
    sum(log_D3_overdue) AS D3_overdue,
    sum(log_D4_overdue) AS D4_overdue,
    sum(log_D5_overdue) AS D5_overdue,
    sum(log_D6_overdue) AS D6_overdue,
    sum(log_D7_overdue) AS D7_overdue,
    sum(log_D15_overdue) AS D15_overdue,
    sum(log_D30_overdue) AS D30_overdue,
    sum(log_D60_overdue) AS D60_overdue,
    sum(log_D90_overdue) AS D90_overdue
from dm_aifox.jf_asset_group
where is_D1_gt0=1
-- and is_new_overdue='是'
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16
union all

select
    "金额" as stat_type,
    'all' as overdue_type,
    delay_due_month,
    due_week_star,
     cast(delay_due_time as date) as delay_due_time,
    due_day,
    if_api,
    user_debt_status,
    product,
    period_seq,
    apply_channel_source,
    amt_type_1,
    log_type,
     is_new_overdue,
        is_asset_new_overdue,
    'all' as C_level,
    0 as C_score,
    sum(due_cnt) AS due_cnt,
    sum(D_7_overdue) AS D_7_overdue,
    sum(D_6_overdue) AS D_6_overdue,
    sum(D_5_overdue) AS D_5_overdue,
    sum(D_4_overdue) AS D_4_overdue,
    sum(D_3_overdue) AS D_3_overdue,
    sum(D_2_overdue) AS D_2_overdue,
    sum(D_1_overdue) AS D_1_overdue,
    sum(D0_overdue) AS D0_overdue,
    sum(D1_overdue) AS D1_overdue,
    sum(D2_overdue) AS D2_overdue,
    sum(D3_overdue) AS D3_overdue,
    sum(D4_overdue) AS D4_overdue,
    sum(D5_overdue) AS D5_overdue,
    sum(D6_overdue) AS D6_overdue,
    sum(D7_overdue) AS D7_overdue,
    sum(D15_overdue) AS D15_overdue,
    sum(D30_overdue) AS D30_overdue,
    sum(D60_overdue) AS D60_overdue,
    sum(D90_overdue) AS D90_overdue
from dm_aifox.jf_asset_group
where 1=1
-- and is_new_overdue='是'
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16
union all
select
    "件数" as stat_type,
    'all' as overdue_type,
    delay_due_month,
    due_week_star,
     cast(delay_due_time as date) as delay_due_time,
    due_day,
    if_api,
    user_debt_status,
    product,
    period_seq,
    apply_channel_source,
    amt_type_1,
    log_type,
     is_new_overdue,
        is_asset_new_overdue,
    'all' as C_level,
    0 as C_score,
  sum(log_due_cnt) AS due_cnt,
    sum(log_D_7_overdue) AS D_7_overdue,
    sum(log_D_6_overdue) AS D_6_overdue,
    sum(log_D_5_overdue) AS D_5_overdue,
    sum(log_D_4_overdue) AS D_4_overdue,
    sum(log_D_3_overdue) AS D_3_overdue,
    sum(log_D_2_overdue) AS D_2_overdue,
    sum(log_D_1_overdue) AS D_1_overdue,
    sum(log_D0_overdue) AS D0_overdue,
    sum(log_D1_overdue) AS D1_overdue,
    sum(log_D2_overdue) AS D2_overdue,
    sum(log_D3_overdue) AS D3_overdue,
    sum(log_D4_overdue) AS D4_overdue,
    sum(log_D5_overdue) AS D5_overdue,
    sum(log_D6_overdue) AS D6_overdue,
    sum(log_D7_overdue) AS D7_overdue,
    sum(log_D15_overdue) AS D15_overdue,
    sum(log_D30_overdue) AS D30_overdue,
    sum(log_D60_overdue) AS D60_overdue,
    sum(log_D90_overdue) AS D90_overdue
from dm_aifox.jf_asset_group
where 1=1
-- and is_new_overdue='是'
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16;





