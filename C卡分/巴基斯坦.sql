drop table if exists dm_aifox.pak_asset_c_card_1_jf;
SET query_timeout = 1800;

create table if not exists dm_aifox.pak_asset_c_card_1_jf as
with asset_detail as (
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
date_format(a.delay_due_time,'%Y-%m') AS delay_due_month,
a.period_seq,
DATE(
      DATE_ADD(
        DATE(a.delay_due_time),
        INTERVAL - (
          CASE
            WHEN DAYOFWEEK(a.delay_due_time) = 5 THEN 0
            WHEN DAYOFWEEK(a.delay_due_time) > 5 THEN DAYOFWEEK(a.delay_due_time) - 5
            ELSE DAYOFWEEK(a.delay_due_time) + 2
          END
        ) DAY
      )
    ) as due_week_star,
date_format(a.delay_due_time,'%Y-%m-%d') AS delay_due_time,
date_format(a.finish_time,'%Y-%m-%d') AS finish_time,
a.asset_overdue_period_days,
case when a.asset_overdue_period_days in (0,1) then '是'
     when a.asset_overdue_period_days is null then '未到期'
     else '否' end asset_new_due,
case when a.overdue_period_days in (0,1) then '是'
     when a.overdue_period_days is null then '未到期'
     else '否' end debtor_new_due,
case when a.granted_principal_period_amt<=3000 then '(0,3000]'
   when a.granted_principal_period_amt<=10000 then '(3000,10000]'
   else '(10000+' end as amt_type_1,
case when a.granted_principal_period_amt<=1000 then '(0,1000]'
   when a.granted_principal_period_amt<=2000 then '(1000,2000]'
   when a.granted_principal_period_amt<=3000 then '(2000,3000]'
   when a.granted_principal_period_amt<=4000 then '(3000,4000]'
   when a.granted_principal_period_amt<=5000 then '(4000,5000]'
   when a.granted_principal_period_amt<=6000 then '(5000,6000]'
   when a.granted_principal_period_amt<=7000 then '(6000,7000]'
   when a.granted_principal_period_amt<=8000 then '(7000,8000]'
   when a.granted_principal_period_amt<=9000 then '(8000,9000]'
   when a.granted_principal_period_amt<=10000 then '(9000,10000]'
else '(10000+)' end as amt_type_2,
a.granted_principal_period_amt,
a.repaid_principal_period_amt
FROM bi.dwb.dwb_asset_period_info AS a
INNER JOIN bi.dwb.dwb_asset_info AS c ON
a.asset_item_no = c.asset_item_no
WHERE 1=1
and a.grant_time >= '2023-01-01'
AND a.asset_overdue_period_days IN (0, 1)
AND date_format(a.delay_due_time,'%Y-%m-01') >= '2025-01-01'
AND a.delay_due_time < curdate()
)
, d3level as (
 select
   asset_item_no,
   date(etl_time) as dt,
   due_diff_days,
   asset_period,
   date(asset_due_at) as due_dt,
   max(level) as max_D3_level,
   max(score) as max_D3_score
 from internal.rpt.rpt_w_cscore_model_result_v2
 where substr(etl_time,1,7) >= '2024-10-01'
 and due_diff_days = 3
 group by 1,2,3,4,5)
 -- select * from d4level limit 10
, d1level as (
 select
   asset_item_no,
   date(etl_time) as dt,
   due_diff_days,
   asset_period,
   date(asset_due_at) as due_dt,
   max(level) as max_D1_level,
   max(score) as max_D1_score
 from internal.rpt.rpt_w_cscore_model_result_v2
 where substr(etl_time,1,7) >= '2024-10-01'
 and due_diff_days = 1
 group by 1,2,3,4,5)
, d0level as (
 select
   asset_item_no,
   date(etl_time) as dt,
   due_diff_days,
   asset_period,
   date(asset_due_at) as due_dt,
   max(level) as max_D0_level,
   max(score) as max_D0_score
 from internal.rpt.rpt_w_cscore_model_result_v2
 where substr(etl_time,1,7) >=  '2024-10-01'
 and due_diff_days = 0
 group by 1,2,3,4,5)
, d_2level as (
 select
   asset_item_no,
   date(etl_time) as dt,
   due_diff_days,
   asset_period,
   date(asset_due_at) as due_dt,
   max(level) as max_D_2_level,
   max(score) as max_D_2_score
 from internal.rpt.rpt_w_cscore_model_result_v2
 where substr(etl_time,1,7) >= '2024-10-01'
 and due_diff_days = -2
 group by 1,2,3,4,5)
-- 明细叠加
,day_detial as(
select
a.user_id,
a.asset_item_no,
a.user_status,
a.user_debt_status,
date_format(a.grant_time,'%Y-%m') AS grant_mth,
a.grant_time,
date(a.delay_due_time) as delay_due_time,
day(a.delay_due_time) as due_day,
a.due_week_star,
a.delay_due_month,
a.finish_time,
a.asset_overdue_period_days,
a.date_diff,
a.amt_type_1,
a.amt_type_2,
a.granted_principal_period_amt,
a.repaid_principal_period_amt,
a.product,
a.period_seq,
a.product_form,
a.apply_channel_source,
a.asset_loan_channel,
a.debtor_new_due,
a.asset_new_due,
d.max_D_2_level,
b.max_D0_level,
c.max_D1_level,
e.max_D3_level,
cast(d.max_D_2_score as float) as max_D_2_score,
cast(b.max_D0_score as float) as max_D0_score ,
cast(c.max_D1_score as float) as max_D1_score,
cast(e.max_D3_score as float) as max_D3_score,
if(date(a.delay_due_time) < curdate(), a.granted_principal_period_amt, 0) AS due_cnt,  -- 总到期本金
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 7 DAY) AND (date_diff IS NULL OR date_diff > -7), a.granted_principal_period_amt, 0) AS D_7_overdue, -- D-7未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 6 DAY) AND (date_diff IS NULL OR date_diff > -6), a.granted_principal_period_amt, 0) AS D_6_overdue, -- D-6未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 5 DAY) AND (date_diff IS NULL OR date_diff > -5), a.granted_principal_period_amt, 0) AS D_5_overdue, -- D-5未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 4 DAY) AND (date_diff IS NULL OR date_diff > -4), a.granted_principal_period_amt, 0) AS D_4_overdue, -- D-4未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 3 DAY) AND (date_diff IS NULL OR date_diff > -3), a.granted_principal_period_amt, 0) AS D_3_overdue, -- D-3未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 2 DAY) AND (date_diff IS NULL OR date_diff > -2), a.granted_principal_period_amt, 0) AS D_2_overdue, -- D-2未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL 1 DAY) AND (date_diff IS NULL OR date_diff > -1), a.granted_principal_period_amt, 0) AS D_1_overdue, -- D-1未还
if(date(a.delay_due_time) < curdate() AND (date_diff IS NULL OR date_diff > 0), a.granted_principal_period_amt, 0) AS D0_overdue, -- D0未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -1 DAY) AND (date_diff IS NULL OR date_diff > 1), a.granted_principal_period_amt, 0) AS D1_overdue, -- D1未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -2 DAY) AND (date_diff IS NULL OR date_diff > 2), a.granted_principal_period_amt, 0) AS D2_overdue, -- D2未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff > 3), a.granted_principal_period_amt, 0) AS D3_overdue, -- D3未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -4 DAY) AND (date_diff IS NULL OR date_diff > 4), a.granted_principal_period_amt, 0) AS D4_overdue, -- D4未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -5 DAY) AND (date_diff IS NULL OR date_diff > 5), a.granted_principal_period_amt, 0) AS D5_overdue, -- D5未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -6 DAY) AND (date_diff IS NULL OR date_diff > 6), a.granted_principal_period_amt, 0) AS D6_overdue, -- D6未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff > 7), a.granted_principal_period_amt, 0) AS D7_overdue, -- D7未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff > 15), a.granted_principal_period_amt, 0) AS D15_overdue, -- D15未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff > 30), a.granted_principal_period_amt, 0) AS D30_overdue, -- D30未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff > 60), a.granted_principal_period_amt, 0) AS D60_overdue, -- D60未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff > 90), a.granted_principal_period_amt, 0) AS D90_overdue -- D90未还
from asset_detail as a
left join d0level as b
on a.asset_item_no = b.asset_item_no
and a.period_seq = b.asset_period
and a.delay_due_time=b.dt
left join d1level as c
on a.asset_item_no = c.asset_item_no
and a.period_seq = c.asset_period
and date_add(a.delay_due_time, INTERVAL 1 DAY)=c.dt
left join d_2level as d
on a.asset_item_no = d.asset_item_no
and a.period_seq = d.asset_period
and date_add(a.delay_due_time, INTERVAL -2 DAY)=d.dt
left join d3level as e
on a.asset_item_no = e.asset_item_no
and a.period_seq = e.asset_period
and date_add(a.delay_due_time, INTERVAL 3 DAY)=e.dt
where 1=1
)

-- 汇总
select
"金额" as stat_type,
'D-2' as overdue_type,
delay_due_month,
due_week_star,
delay_due_time,
due_day,
user_status,
user_debt_status,
product,
period_seq,
apply_channel_source,
amt_type_1,
amt_type_2,
debtor_new_due,
asset_new_due,
max_D_2_level as C_level,
sum(coalesce(max_D_2_score ,0)) as C_score,
sum(due_cnt) AS due_cnt,  -- 总到期本金
sum(D_7_overdue) AS D_7_overdue, -- D-7未还
sum(D_6_overdue) AS D_6_overdue, -- D-7未还
sum(D_5_overdue) AS D_5_overdue, -- D-7未还
sum(D_4_overdue) AS D_4_overdue, -- D-7未还
sum(D_3_overdue) AS D_3_overdue, -- D-7未还
sum(D_2_overdue) AS D_2_overdue, -- D-7未还
sum(D_1_overdue) AS D_1_overdue, -- D-7未还
sum(D0_overdue) AS D0_overdue, -- D0未还
sum(D1_overdue) AS D1_overdue, -- D1未还
sum(D2_overdue) AS D2_overdue, -- D1未还
sum(D3_overdue) AS D3_overdue, -- D1未还
sum(D4_overdue) AS D4_overdue, -- D1未还
sum(D5_overdue) AS D5_overdue, -- D1未还
sum(D6_overdue) AS D6_overdue, -- D1未还
sum(D7_overdue) AS D7_overdue, -- D7逾期未还
sum(D15_overdue) AS D15_overdue, -- D7逾期未还
sum(D30_overdue) AS D30_overdue, -- D7逾期未还
sum(D60_overdue) AS D60_overdue, -- D7逾期未还
sum(D90_overdue) AS D90_overdue -- D7逾期未还
from day_detial
where D_3_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16


union all
select
"件数" as stat_type,
'D-2' as overdue_type,
delay_due_month,
due_week_star,
delay_due_time,
due_day,
user_status,
user_debt_status,
product,
period_seq,
apply_channel_source,
amt_type_1,
amt_type_2,
debtor_new_due,
asset_new_due,
max_D_2_level as C_level,
sum(coalesce(max_D_2_score ,0)) as C_score,
count(distinct case when date(delay_due_time) < curdate() then asset_item_no end ) AS due_cnt,  -- 总到期客户数
count(distinct case when D_7_overdue>0  then asset_item_no end) AS D_7_overdue, -- D1未还
count(distinct case when D_6_overdue>0  then asset_item_no end) AS D_6_overdue, -- D1未还
count(distinct case when D_5_overdue>0  then asset_item_no end) AS D_5_overdue, -- D1未还
count(distinct case when D_4_overdue>0  then asset_item_no end) AS D_4_overdue, -- D1未还
count(distinct case when D_3_overdue>0 then asset_item_no end) AS D_3_overdue, -- D1未还
count(distinct case when D_2_overdue>0  then asset_item_no end) AS D_2_overdue, -- D1未还
count(distinct case when D_1_overdue>0  then asset_item_no end) AS D_1_overdue, -- D1未还
count(distinct case when D0_overdue>0  then asset_item_no end ) AS D0_overdue, -- D0未还
count(distinct case when D1_overdue>0  then asset_item_no end) AS D1_overdue, -- D1未还
count(distinct case when D2_overdue>0 then asset_item_no end) AS D2_overdue,-- D7逾期未还
count(distinct case when D3_overdue>0 then asset_item_no end) AS D3_overdue,-- D7逾期未还
count(distinct case when D4_overdue>0 then asset_item_no end) AS D4_overdue,-- D7逾期未还
count(distinct case when D5_overdue>0 then asset_item_no end) AS D5_overdue,
count(distinct case when D6_overdue>0 then asset_item_no end) AS D6_overdue,
count(distinct case when D7_overdue>0 then asset_item_no end) AS D7_overdue,
count(distinct case when D15_overdue>0 then asset_item_no end) AS D15_overdue,
count(distinct case when D30_overdue>0 then asset_item_no end) AS D30_overdue,
count(distinct case when D60_overdue>0 then asset_item_no end) AS D60_overdue,
count(distinct case when D90_overdue>0 then asset_item_no end) AS D90_overdue
from day_detial
where D_3_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

union all

select
"金额" as stat_type,
'D0' as overdue_type,
delay_due_month,
due_week_star,
delay_due_time,
due_day,
user_status,
user_debt_status,
product,
period_seq,
apply_channel_source,
amt_type_1,
amt_type_2,
debtor_new_due,
asset_new_due,
max_D0_level as C_level,
sum(coalesce(max_D0_score ,0)) as C_score,
sum(due_cnt) AS due_cnt,  -- 总到期本金
sum(D_7_overdue) AS D_7_overdue, -- D-7未还
sum(D_6_overdue) AS D_6_overdue, -- D-7未还
sum(D_5_overdue) AS D_5_overdue, -- D-7未还
sum(D_4_overdue) AS D_4_overdue, -- D-7未还
sum(D_3_overdue) AS D_3_overdue, -- D-7未还
sum(D_2_overdue) AS D_2_overdue, -- D-7未还
sum(D_1_overdue) AS D_1_overdue, -- D-7未还
sum(D0_overdue) AS D0_overdue, -- D0未还
sum(D1_overdue) AS D1_overdue, -- D1未还
sum(D2_overdue) AS D2_overdue, -- D1未还
sum(D3_overdue) AS D3_overdue, -- D1未还
sum(D4_overdue) AS D4_overdue, -- D1未还
sum(D5_overdue) AS D5_overdue, -- D1未还
sum(D6_overdue) AS D6_overdue, -- D1未还
sum(D7_overdue) AS D7_overdue, -- D7逾期未还
sum(D15_overdue) AS D15_overdue, -- D7逾期未还
sum(D30_overdue) AS D30_overdue, -- D7逾期未还
sum(D60_overdue) AS D60_overdue, -- D7逾期未还
sum(D90_overdue) AS D90_overdue -- D7逾期未还
from day_detial
where D_1_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
union all
select
"件数" as stat_type,
'D0' as overdue_type,
delay_due_month,
due_week_star,
delay_due_time,
due_day,
user_status,
user_debt_status,
product,
period_seq,
apply_channel_source,
amt_type_1,
amt_type_2,
debtor_new_due,
asset_new_due,
max_D0_level as C_level,
sum(coalesce(max_D0_score ,0)) as C_score,
count(distinct case when date(delay_due_time) < curdate() then asset_item_no end ) AS due_cnt,  -- 总到期客户数
count(distinct case when D_7_overdue>0  then asset_item_no end) AS D_7_overdue, -- D1未还
count(distinct case when D_6_overdue>0  then asset_item_no end) AS D_6_overdue, -- D1未还
count(distinct case when D_5_overdue>0  then asset_item_no end) AS D_5_overdue, -- D1未还
count(distinct case when D_4_overdue>0  then asset_item_no end) AS D_4_overdue, -- D1未还
count(distinct case when D_3_overdue>0 then asset_item_no end) AS D_3_overdue, -- D1未还
count(distinct case when D_2_overdue>0  then asset_item_no end) AS D_2_overdue, -- D1未还
count(distinct case when D_1_overdue>0  then asset_item_no end) AS D_1_overdue, -- D1未还
count(distinct case when D0_overdue>0  then asset_item_no end ) AS D0_overdue, -- D0未还
count(distinct case when D1_overdue>0  then asset_item_no end) AS D1_overdue, -- D1未还
count(distinct case when D2_overdue>0 then asset_item_no end) AS D2_overdue,-- D7逾期未还
count(distinct case when D3_overdue>0 then asset_item_no end) AS D3_overdue,-- D7逾期未还
count(distinct case when D4_overdue>0 then asset_item_no end) AS D4_overdue,-- D7逾期未还
count(distinct case when D5_overdue>0 then asset_item_no end) AS D5_overdue,
count(distinct case when D6_overdue>0 then asset_item_no end) AS D6_overdue,
count(distinct case when D7_overdue>0 then asset_item_no end) AS D7_overdue,
count(distinct case when D15_overdue>0 then asset_item_no end) AS D15_overdue,
count(distinct case when D30_overdue>0 then asset_item_no end) AS D30_overdue,
count(distinct case when D60_overdue>0 then asset_item_no end) AS D60_overdue,
count(distinct case when D90_overdue>0 then asset_item_no end) AS D90_overdue
from day_detial
where D_1_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

union all

select
"金额" as stat_type,
'D1' as overdue_type,
delay_due_month,
due_week_star,
delay_due_time,
due_day,
user_status,
user_debt_status,
product,
period_seq,
apply_channel_source,
amt_type_1,
amt_type_2,
debtor_new_due,
asset_new_due,
max_D1_level as C_level,
sum(coalesce(max_D1_score ,0)) as C_score,
sum(due_cnt) AS due_cnt,  -- 总到期本金
sum(D_7_overdue) AS D_7_overdue, -- D-7未还
sum(D_6_overdue) AS D_6_overdue, -- D-7未还
sum(D_5_overdue) AS D_5_overdue, -- D-7未还
sum(D_4_overdue) AS D_4_overdue, -- D-7未还
sum(D_3_overdue) AS D_3_overdue, -- D-7未还
sum(D_2_overdue) AS D_2_overdue, -- D-7未还
sum(D_1_overdue) AS D_1_overdue, -- D-7未还
sum(D0_overdue) AS D0_overdue, -- D0未还
sum(D1_overdue) AS D1_overdue, -- D1未还
sum(D2_overdue) AS D2_overdue, -- D1未还
sum(D3_overdue) AS D3_overdue, -- D1未还
sum(D4_overdue) AS D4_overdue, -- D1未还
sum(D5_overdue) AS D5_overdue, -- D1未还
sum(D6_overdue) AS D6_overdue, -- D1未还
sum(D7_overdue) AS D7_overdue, -- D7逾期未还
sum(D15_overdue) AS D15_overdue, -- D7逾期未还
sum(D30_overdue) AS D30_overdue, -- D7逾期未还
sum(D60_overdue) AS D60_overdue, -- D7逾期未还
sum(D90_overdue) AS D90_overdue -- D7逾期未还
from day_detial
where D0_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

union all

select
"件数" as stat_type,
'D1' as overdue_type,
delay_due_month,
due_week_star,
delay_due_time,
due_day,
user_status,
user_debt_status,
product,
period_seq,
apply_channel_source,
amt_type_1,
amt_type_2,
debtor_new_due,
asset_new_due,
max_D1_level as C_level,
sum(coalesce(max_D1_score ,0)) as C_score,
count(distinct case when date(delay_due_time) < curdate() then asset_item_no end ) AS due_cnt,  -- 总到期客户数
count(distinct case when D_7_overdue>0  then asset_item_no end) AS D_7_overdue, -- D1未还
count(distinct case when D_6_overdue>0  then asset_item_no end) AS D_6_overdue, -- D1未还
count(distinct case when D_5_overdue>0  then asset_item_no end) AS D_5_overdue, -- D1未还
count(distinct case when D_4_overdue>0  then asset_item_no end) AS D_4_overdue, -- D1未还
count(distinct case when D_3_overdue>0 then asset_item_no end) AS D_3_overdue, -- D1未还
count(distinct case when D_2_overdue>0  then asset_item_no end) AS D_2_overdue, -- D1未还
count(distinct case when D_1_overdue>0  then asset_item_no end) AS D_1_overdue, -- D1未还
count(distinct case when D0_overdue>0  then asset_item_no end ) AS D0_overdue, -- D0未还
count(distinct case when D1_overdue>0  then asset_item_no end) AS D1_overdue, -- D1未还
count(distinct case when D2_overdue>0 then asset_item_no end) AS D2_overdue,-- D7逾期未还
count(distinct case when D3_overdue>0 then asset_item_no end) AS D3_overdue,-- D7逾期未还
count(distinct case when D4_overdue>0 then asset_item_no end) AS D4_overdue,-- D7逾期未还
count(distinct case when D5_overdue>0 then asset_item_no end) AS D5_overdue,
count(distinct case when D6_overdue>0 then asset_item_no end) AS D6_overdue,
count(distinct case when D7_overdue>0 then asset_item_no end) AS D7_overdue,
count(distinct case when D15_overdue>0 then asset_item_no end) AS D15_overdue,
count(distinct case when D30_overdue>0 then asset_item_no end) AS D30_overdue,
count(distinct case when D60_overdue>0 then asset_item_no end) AS D60_overdue,
count(distinct case when D90_overdue>0 then asset_item_no end) AS D90_overdue
from day_detial
where D0_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

union all

select
"金额" as stat_type,
'D3' as overdue_type,
delay_due_month,
due_week_star,
delay_due_time,
due_day,
user_status,
user_debt_status,
product,
period_seq,
apply_channel_source,
amt_type_1,
amt_type_2,
debtor_new_due,
asset_new_due,
max_D3_level as C_level,
sum(coalesce(max_D3_score ,0)) as C_score,
sum(due_cnt) AS due_cnt,  -- 总到期本金
sum(D_7_overdue) AS D_7_overdue, -- D-7未还
sum(D_6_overdue) AS D_6_overdue, -- D-7未还
sum(D_5_overdue) AS D_5_overdue, -- D-7未还
sum(D_4_overdue) AS D_4_overdue, -- D-7未还
sum(D_3_overdue) AS D_3_overdue, -- D-7未还
sum(D_2_overdue) AS D_2_overdue, -- D-7未还
sum(D_1_overdue) AS D_1_overdue, -- D-7未还
sum(D0_overdue) AS D0_overdue, -- D0未还
sum(D1_overdue) AS D1_overdue, -- D1未还
sum(D2_overdue) AS D2_overdue, -- D1未还
sum(D3_overdue) AS D3_overdue, -- D1未还
sum(D4_overdue) AS D4_overdue, -- D1未还
sum(D5_overdue) AS D5_overdue, -- D1未还
sum(D6_overdue) AS D6_overdue, -- D1未还
sum(D7_overdue) AS D7_overdue, -- D7逾期未还
sum(D15_overdue) AS D15_overdue, -- D7逾期未还
sum(D30_overdue) AS D30_overdue, -- D7逾期未还
sum(D60_overdue) AS D60_overdue, -- D7逾期未还
sum(D90_overdue) AS D90_overdue -- D7逾期未还
from day_detial
where D2_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

union all

select
"件数" as stat_type,
'D3' as overdue_type,
delay_due_month,
due_week_star,
delay_due_time,
due_day,
user_status,
user_debt_status,
product,
period_seq,
apply_channel_source,
amt_type_1,
amt_type_2,
debtor_new_due,
asset_new_due,
max_D3_level as C_level,
sum(coalesce(max_D3_score ,0)) as C_score,
count(distinct case when date(delay_due_time) < curdate() then asset_item_no end ) AS due_cnt,  -- 总到期客户数
count(distinct case when D_7_overdue>0  then asset_item_no end) AS D_7_overdue, -- D1未还
count(distinct case when D_6_overdue>0  then asset_item_no end) AS D_6_overdue, -- D1未还
count(distinct case when D_5_overdue>0  then asset_item_no end) AS D_5_overdue, -- D1未还
count(distinct case when D_4_overdue>0  then asset_item_no end) AS D_4_overdue, -- D1未还
count(distinct case when D_3_overdue>0 then asset_item_no end) AS D_3_overdue, -- D1未还
count(distinct case when D_2_overdue>0  then asset_item_no end) AS D_2_overdue, -- D1未还
count(distinct case when D_1_overdue>0  then asset_item_no end) AS D_1_overdue, -- D1未还
count(distinct case when D0_overdue>0  then asset_item_no end ) AS D0_overdue, -- D0未还
count(distinct case when D1_overdue>0  then asset_item_no end) AS D1_overdue, -- D1未还
count(distinct case when D2_overdue>0 then asset_item_no end) AS D2_overdue,-- D7逾期未还
count(distinct case when D3_overdue>0 then asset_item_no end) AS D3_overdue,-- D7逾期未还
count(distinct case when D4_overdue>0 then asset_item_no end) AS D4_overdue,-- D7逾期未还
count(distinct case when D5_overdue>0 then asset_item_no end) AS D5_overdue,
count(distinct case when D6_overdue>0 then asset_item_no end) AS D6_overdue,
count(distinct case when D7_overdue>0 then asset_item_no end) AS D7_overdue,
count(distinct case when D15_overdue>0 then asset_item_no end) AS D15_overdue,
count(distinct case when D30_overdue>0 then asset_item_no end) AS D30_overdue,
count(distinct case when D60_overdue>0 then asset_item_no end) AS D60_overdue,
count(distinct case when D90_overdue>0 then asset_item_no end) AS D90_overdue
from day_detial
where D2_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

union all

select
"金额" as stat_type,
'all' as overdue_type,
delay_due_month,
due_week_star,
delay_due_time,
due_day,
user_status,
user_debt_status,
product,
period_seq,
apply_channel_source,
amt_type_1,
amt_type_2,
debtor_new_due,
asset_new_due,
100 as C_level,
100 as C_score,
sum(due_cnt) AS due_cnt,  -- 总到期本金
sum(D_7_overdue) AS D_7_overdue, -- D-7未还
sum(D_6_overdue) AS D_6_overdue, -- D-7未还
sum(D_5_overdue) AS D_5_overdue, -- D-7未还
sum(D_4_overdue) AS D_4_overdue, -- D-7未还
sum(D_3_overdue) AS D_3_overdue, -- D-7未还
sum(D_2_overdue) AS D_2_overdue, -- D-7未还
sum(D_1_overdue) AS D_1_overdue, -- D-7未还
sum(D0_overdue) AS D0_overdue, -- D0未还
sum(D1_overdue) AS D1_overdue, -- D1未还
sum(D2_overdue) AS D2_overdue, -- D1未还
sum(D3_overdue) AS D3_overdue, -- D1未还
sum(D4_overdue) AS D4_overdue, -- D1未还
sum(D5_overdue) AS D5_overdue, -- D1未还
sum(D6_overdue) AS D6_overdue, -- D1未还
sum(D7_overdue) AS D7_overdue, -- D7逾期未还
sum(D15_overdue) AS D15_overdue, -- D7逾期未还
sum(D30_overdue) AS D30_overdue, -- D7逾期未还
sum(D60_overdue) AS D60_overdue, -- D7逾期未还
sum(D90_overdue) AS D90_overdue -- D7逾期未还
from day_detial
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

union all

select
"件数" as stat_type,
'all' as overdue_type,
delay_due_month,
due_week_star,
delay_due_time,
due_day,
user_status,
user_debt_status,
product,
period_seq,
apply_channel_source,
amt_type_1,
amt_type_2,
debtor_new_due,
asset_new_due,
100 as C_level,
100 as C_score,
count(distinct case when date(delay_due_time) < curdate() then asset_item_no end ) AS due_cnt,  -- 总到期客户数
count(distinct case when D_7_overdue>0  then asset_item_no end) AS D_7_overdue, -- D1未还
count(distinct case when D_6_overdue>0  then asset_item_no end) AS D_6_overdue, -- D1未还
count(distinct case when D_5_overdue>0  then asset_item_no end) AS D_5_overdue, -- D1未还
count(distinct case when D_4_overdue>0  then asset_item_no end) AS D_4_overdue, -- D1未还
count(distinct case when D_3_overdue>0 then asset_item_no end) AS D_3_overdue, -- D1未还
count(distinct case when D_2_overdue>0  then asset_item_no end) AS D_2_overdue, -- D1未还
count(distinct case when D_1_overdue>0  then asset_item_no end) AS D_1_overdue, -- D1未还
count(distinct case when D0_overdue>0  then asset_item_no end ) AS D0_overdue, -- D0未还
count(distinct case when D1_overdue>0  then asset_item_no end) AS D1_overdue, -- D1未还
count(distinct case when D2_overdue>0 then asset_item_no end) AS D2_overdue,-- D7逾期未还
count(distinct case when D3_overdue>0 then asset_item_no end) AS D3_overdue,-- D7逾期未还
count(distinct case when D4_overdue>0 then asset_item_no end) AS D4_overdue,-- D7逾期未还
count(distinct case when D5_overdue>0 then asset_item_no end) AS D5_overdue,
count(distinct case when D6_overdue>0 then asset_item_no end) AS D6_overdue,
count(distinct case when D7_overdue>0 then asset_item_no end) AS D7_overdue,
count(distinct case when D15_overdue>0 then asset_item_no end) AS D15_overdue,
count(distinct case when D30_overdue>0 then asset_item_no end) AS D30_overdue,
count(distinct case when D60_overdue>0 then asset_item_no end) AS D60_overdue,
count(distinct case when D90_overdue>0 then asset_item_no end) AS D90_overdue
from day_detial
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

