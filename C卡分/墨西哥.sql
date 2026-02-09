


drop table if exists dm_aifox.mex_asset_c_card_base_jf;
create table dm_aifox.mex_asset_c_card_base_jf as
select asset_item_no,
       date(dt) as dt,
       due_diff_days,
       asset_period,
       date(asset_due_at) as due_dt,
       max(level) as max_level,
       max(score) as max_score
  from dwd.dwd_fox_c_score_model_result
 where dt >= '2025-01-01'
   --and due_diff_days = 3
 group by 1,2,3,4,5;














drop table if exists dm_aifox.mex_asset_c_card_2_jf;
create table if not exists dm_aifox.mex_asset_c_card_2_jf as
with asset_detail as (
select a.asset_item_no,
a.user_id,
if(a.user_debt_status='new_user','new','old') AS user_status,
a.user_debt_status,
a.apply_channel_source,
c.asset_loan_channel,
c.product_form,
c.product_period_unit,
case when a.asset_overdue_period_days in (0,1) then '是'
     when a.asset_overdue_period_days is null then '未到期'
     else '否' end asset_new_due,
case when a.overdue_period_days in (0,1) then '是'
     when a.overdue_period_days is null then '未到期'
     else '否' end debtor_new_due,
concat(c.product_period_qty,'天','*',c.product_period_cnt,'期') as product,
date_format(a.grant_time,'%Y-%m-%d') AS grant_time,
datediff(a.finish_time,a.delay_due_time) AS date_diff,
date_format(a.delay_due_time,'%Y-%m-01') AS delay_due_month,
a.period_seq,
date_format(date(date_add(date(a.delay_due_time), INTERVAL  if(DAYOFWEEK(a.delay_due_time) in(5,6,7),-DAYOFWEEK(a.delay_due_time)+5, -2 -DAYOFWEEK(a.delay_due_time)) day )), '%Y-%m-%d') as due_week_star,
date_format(a.delay_due_time,'%Y-%m-%d')  AS delay_due_time,
date_format(a.finish_time,'%Y-%m-%d') AS finish_time,
a.overdue_period_days,
case when a.granted_principal_period_amt>1000 then '(1000+)' else '(0,1000]' end as amt_type_1,
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
a.repaid_principal_period_amt
FROM dwb.dwb_asset_period_info AS a
INNER JOIN dwb.dwb_asset_info AS c ON
a.asset_item_no = c.asset_item_no
WHERE 1=1
AND date_format(a.delay_due_time,'%Y-%m-01') >= '2025-01-01'
and a.grant_time >= '2023-01-01'
AND a.delay_due_time < curdate()
)

,day_detial as(
select
a.user_id,
a.asset_item_no,
a.user_status,
a.user_debt_status,
date_format(a.grant_time,'%Y-%m') AS grant_mth,
a.grant_time,
cast(a.delay_due_time as date) as delay_due_time,
day(a.delay_due_time) as due_day,
a.due_week_star,
a.delay_due_month,
a.finish_time,
a.overdue_period_days,
a.debtor_new_due,
a.asset_new_due,
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
d.max_level max_D_2_level,
b.max_level max_D0_level,
c.max_level max_D1_level,
e.max_level max_D3_level,
cast(d.max_score as float) as max_D_2_score ,
cast(b.max_score as float) as max_D0_score,
cast(c.max_score as float) as max_D1_score,
cast(e.max_score as float) as max_D3_score,
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
left join (select * from dm_aifox.mex_asset_c_card_base where due_diff_days=0) as b
on a.asset_item_no = b.asset_item_no
and a.period_seq = b.asset_period
and a.delay_due_time=b.dt
left join (select * from dm_aifox.mex_asset_c_card_base where due_diff_days=1) as c
on a.asset_item_no = c.asset_item_no
and a.period_seq = c.asset_period
and date_add(a.delay_due_time, INTERVAL 1 DAY)=c.dt
left join (select * from dm_aifox.mex_asset_c_card_base where due_diff_days=-2) as d
on a.asset_item_no = d.asset_item_no
and a.period_seq = d.asset_period
and date_add(a.delay_due_time, INTERVAL -2 DAY)=d.dt
left join (select * from dm_aifox.mex_asset_c_card_base where due_diff_days=3) as e
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
max_D_2_level as C_level,
debtor_new_due,
asset_new_due,
@update_time as update_time,
sum(coalesce(max_D_2_score,0)) as C_score,
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
from day_detial
where D_3_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17


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
max_D_2_level as C_level,
debtor_new_due,
asset_new_due,
@update_time as update_time,
sum(coalesce(max_D_2_score,0)) as C_score,
count(distinct case when date(delay_due_time) < curdate() then asset_item_no end ) AS due_cnt,  
count(distinct case when D_7_overdue>0  then asset_item_no end) AS D_7_overdue, 
count(distinct case when D_6_overdue>0  then asset_item_no end) AS D_6_overdue, 
count(distinct case when D_5_overdue>0  then asset_item_no end) AS D_5_overdue, 
count(distinct case when D_4_overdue>0  then asset_item_no end) AS D_4_overdue, 
count(distinct case when D_3_overdue>0 then asset_item_no end) AS D_3_overdue, 
count(distinct case when D_2_overdue>0  then asset_item_no end) AS D_2_overdue, 
count(distinct case when D_1_overdue>0  then asset_item_no end) AS D_1_overdue, 
count(distinct case when D0_overdue>0  then asset_item_no end ) AS D0_overdue, 
count(distinct case when D1_overdue>0  then asset_item_no end) AS D1_overdue, 
count(distinct case when D2_overdue>0 then asset_item_no end) AS D2_overdue,
count(distinct case when D3_overdue>0 then asset_item_no end) AS D3_overdue,
count(distinct case when D4_overdue>0 then asset_item_no end) AS D4_overdue,
count(distinct case when D5_overdue>0 then asset_item_no end) AS D5_overdue,
count(distinct case when D6_overdue>0 then asset_item_no end) AS D6_overdue,
count(distinct case when D7_overdue>0 then asset_item_no end) AS D7_overdue,
count(distinct case when D15_overdue>0 then asset_item_no end) AS D15_overdue,
count(distinct case when D30_overdue>0 then asset_item_no end) AS D30_overdue,
count(distinct case when D60_overdue>0 then asset_item_no end) AS D60_overdue,
count(distinct case when D90_overdue>0 then asset_item_no end) AS D90_overdue
from day_detial
where D_3_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
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
max_D0_level as C_level,
debtor_new_due,
asset_new_due,
@update_time as update_time,
sum(coalesce(max_D0_score,0)) as C_score,
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
from day_detial
where D_1_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
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
max_D0_level as C_level,
debtor_new_due,
asset_new_due,
@update_time as update_time,
sum(coalesce(max_D0_score,0)) as C_score,
count(distinct case when date(delay_due_time) < curdate() then asset_item_no end ) AS due_cnt,  
count(distinct case when D_7_overdue>0  then asset_item_no end) AS D_7_overdue, 
count(distinct case when D_6_overdue>0  then asset_item_no end) AS D_6_overdue, 
count(distinct case when D_5_overdue>0  then asset_item_no end) AS D_5_overdue, 
count(distinct case when D_4_overdue>0  then asset_item_no end) AS D_4_overdue, 
count(distinct case when D_3_overdue>0 then asset_item_no end) AS D_3_overdue, 
count(distinct case when D_2_overdue>0  then asset_item_no end) AS D_2_overdue, 
count(distinct case when D_1_overdue>0  then asset_item_no end) AS D_1_overdue, 
count(distinct case when D0_overdue>0  then asset_item_no end ) AS D0_overdue, 
count(distinct case when D1_overdue>0  then asset_item_no end) AS D1_overdue, 
count(distinct case when D2_overdue>0 then asset_item_no end) AS D2_overdue,
count(distinct case when D3_overdue>0 then asset_item_no end) AS D3_overdue,
count(distinct case when D4_overdue>0 then asset_item_no end) AS D4_overdue,
count(distinct case when D5_overdue>0 then asset_item_no end) AS D5_overdue,
count(distinct case when D6_overdue>0 then asset_item_no end) AS D6_overdue,
count(distinct case when D7_overdue>0 then asset_item_no end) AS D7_overdue,
count(distinct case when D15_overdue>0 then asset_item_no end) AS D15_overdue,
count(distinct case when D30_overdue>0 then asset_item_no end) AS D30_overdue,
count(distinct case when D60_overdue>0 then asset_item_no end) AS D60_overdue,
count(distinct case when D90_overdue>0 then asset_item_no end) AS D90_overdue
from day_detial
where D_1_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
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
max_D1_level as C_level,
debtor_new_due,
asset_new_due,
@update_time as update_time,
sum(coalesce(max_D1_score,0)) as C_score,
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
from day_detial
where D0_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
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
max_D1_level as C_level,
debtor_new_due,
asset_new_due,
@update_time as update_time,
sum(coalesce(max_D1_score,0)) as C_score,
count(distinct case when date(delay_due_time) < curdate() then asset_item_no end ) AS due_cnt,  
count(distinct case when D_7_overdue>0  then asset_item_no end) AS D_7_overdue, 
count(distinct case when D_6_overdue>0  then asset_item_no end) AS D_6_overdue,
count(distinct case when D_5_overdue>0  then asset_item_no end) AS D_5_overdue,
count(distinct case when D_4_overdue>0  then asset_item_no end) AS D_4_overdue, 
count(distinct case when D_3_overdue>0 then asset_item_no end) AS D_3_overdue, 
count(distinct case when D_2_overdue>0  then asset_item_no end) AS D_2_overdue, 
count(distinct case when D_1_overdue>0  then asset_item_no end) AS D_1_overdue, 
count(distinct case when D0_overdue>0  then asset_item_no end ) AS D0_overdue, 
count(distinct case when D1_overdue>0  then asset_item_no end) AS D1_overdue, 
count(distinct case when D2_overdue>0 then asset_item_no end) AS D2_overdue,
count(distinct case when D3_overdue>0 then asset_item_no end) AS D3_overdue,
count(distinct case when D4_overdue>0 then asset_item_no end) AS D4_overdue,
count(distinct case when D5_overdue>0 then asset_item_no end) AS D5_overdue,
count(distinct case when D6_overdue>0 then asset_item_no end) AS D6_overdue,
count(distinct case when D7_overdue>0 then asset_item_no end) AS D7_overdue,
count(distinct case when D15_overdue>0 then asset_item_no end) AS D15_overdue,
count(distinct case when D30_overdue>0 then asset_item_no end) AS D30_overdue,
count(distinct case when D60_overdue>0 then asset_item_no end) AS D60_overdue,
count(distinct case when D90_overdue>0 then asset_item_no end) AS D90_overdue
from day_detial
where D0_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
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
max_D3_level as C_level,
debtor_new_due,
asset_new_due,
@update_time as update_time,
sum(coalesce(max_D3_score,0)) as C_score,
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
from day_detial
where D2_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
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
max_D3_level as C_level,
debtor_new_due,
asset_new_due,
@update_time as update_time,
sum(coalesce(max_D3_score,0)) as C_score,
count(distinct case when date(delay_due_time) < curdate() then asset_item_no end ) AS due_cnt,  
count(distinct case when D_7_overdue>0  then asset_item_no end) AS D_7_overdue, 
count(distinct case when D_6_overdue>0  then asset_item_no end) AS D_6_overdue, 
count(distinct case when D_5_overdue>0  then asset_item_no end) AS D_5_overdue, 
count(distinct case when D_4_overdue>0  then asset_item_no end) AS D_4_overdue, 
count(distinct case when D_3_overdue>0 then asset_item_no end) AS D_3_overdue, 
count(distinct case when D_2_overdue>0  then asset_item_no end) AS D_2_overdue, 
count(distinct case when D_1_overdue>0  then asset_item_no end) AS D_1_overdue, 
count(distinct case when D0_overdue>0  then asset_item_no end ) AS D0_overdue, 
count(distinct case when D1_overdue>0  then asset_item_no end) AS D1_overdue, 
count(distinct case when D2_overdue>0 then asset_item_no end) AS D2_overdue,
count(distinct case when D3_overdue>0 then asset_item_no end) AS D3_overdue,
count(distinct case when D4_overdue>0 then asset_item_no end) AS D4_overdue,
count(distinct case when D5_overdue>0 then asset_item_no end) AS D5_overdue,
count(distinct case when D6_overdue>0 then asset_item_no end) AS D6_overdue,
count(distinct case when D7_overdue>0 then asset_item_no end) AS D7_overdue,
count(distinct case when D15_overdue>0 then asset_item_no end) AS D15_overdue,
count(distinct case when D30_overdue>0 then asset_item_no end) AS D30_overdue,
count(distinct case when D60_overdue>0 then asset_item_no end) AS D60_overdue,
count(distinct case when D90_overdue>0 then asset_item_no end) AS D90_overdue
from day_detial
where D2_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
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
100 as C_level,
debtor_new_due,
asset_new_due,
@update_time as update_time,
sum(coalesce(max_D0_score,0)) as C_score,
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
from day_detial
-- where D2_overdue>0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
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
100 as C_level,
debtor_new_due,
asset_new_due,
@update_time as update_time,
sum(coalesce(max_D0_score,0)) as C_score,
count(distinct case when date(delay_due_time) < curdate() then asset_item_no end ) AS due_cnt,  
count(distinct case when D_7_overdue>0  then asset_item_no end) AS D_7_overdue, 
count(distinct case when D_6_overdue>0  then asset_item_no end) AS D_6_overdue, 
count(distinct case when D_5_overdue>0  then asset_item_no end) AS D_5_overdue, 
count(distinct case when D_4_overdue>0  then asset_item_no end) AS D_4_overdue, 
count(distinct case when D_3_overdue>0 then asset_item_no end) AS D_3_overdue, 
count(distinct case when D_2_overdue>0  then asset_item_no end) AS D_2_overdue, 
count(distinct case when D_1_overdue>0  then asset_item_no end) AS D_1_overdue, 
count(distinct case when D0_overdue>0  then asset_item_no end ) AS D0_overdue, 
count(distinct case when D1_overdue>0  then asset_item_no end) AS D1_overdue,
count(distinct case when D2_overdue>0 then asset_item_no end) AS D2_overdue,
count(distinct case when D3_overdue>0 then asset_item_no end) AS D3_overdue,
count(distinct case when D4_overdue>0 then asset_item_no end) AS D4_overdue,
count(distinct case when D5_overdue>0 then asset_item_no end) AS D5_overdue,
count(distinct case when D6_overdue>0 then asset_item_no end) AS D6_overdue,
count(distinct case when D7_overdue>0 then asset_item_no end) AS D7_overdue,
count(distinct case when D15_overdue>0 then asset_item_no end) AS D15_overdue,
count(distinct case when D30_overdue>0 then asset_item_no end) AS D30_overdue,
count(distinct case when D60_overdue>0 then asset_item_no end) AS D60_overdue,
count(distinct case when D90_overdue>0 then asset_item_no end) AS D90_overdue
from day_detial
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;
