



--------------------------------------------------------------------------------
drop table if exists dm_aifox.d_1level;
drop table if exists dm_aifox.delay_asset_detail_jf_3_overdue_days;

drop table if exists dm_aifox.delay_asset_detail_jf_4;
drop table if exists dm_aifox.delay_day_detial_jf_4 ;
drop table if exists dm_aifox.tha_delay_asset_jf_4 




----------------------------------计算每笔资产首次展期的逾期天数-------------------------------------
create table if not exists dm_aifox.delay_asset_detail_jf_3_overdue_days as 
SELECT
  asset_item_no,
  -- delay_seq_cnt,
  -- period_seq,
  min(delay_overdue_days) as delay_overdue_days
FROM
  (
    SELECT 
        asset_item_no,
        delay_seq_cnt,
        period_seq,
        DATEDIFF(LEAD(delay_pay_time, 1, NULL) OVER (PARTITION BY asset_item_no,period_seq ORDER BY delay_seq_cnt),delay_due_time) AS delay_overdue_days
    FROM
        dwb.dwb_asset_delay_period_info 
    WHERE 1=1
    and grant_time >= '2023-01-01'
    AND due_time >= '2025-06-01'
  ) temp
where
  temp.delay_seq_cnt = 0
  and delay_overdue_days >= -1
GROUP by 1



------------------D0C卡分----------------------

create table if not exists dm_aifox.d0level as 
  select
    asset_item_no,
    date(create_time) as dt,
    due_diff_days,
    asset_period,
    date(asset_due_at) as due_dt,
    max(level) as max_D0_level,
    max(score) as max_D0_score
  from dwd.dwd_fox_c_score_model_result
  where create_time >= date_format(date_add(CURDATE(),interval -14 month),'%Y-%m-01')
  and due_diff_days = 0
  group by 1,2,3,4,5




  -- select
  -- *
  -- from dwd.dwd_fox_c_score_model_result
  -- where asset_item_no = 'T2023072847386351920'






-----------------------资产账单明细关联展期逾期阶段及产品信息-------------------



create table if not exists dm_aifox.delay_asset_detail_jf_4 as 
select 
  a.asset_item_no,
  a.user_id,
  if(a.user_debt_status='new_user','new','old') AS user_status,
  a.user_debt_status,
  a.apply_channel_source,
  c.product_period as product,
  date_format(a.grant_time,'%Y-%m-%d') AS grant_time,
  date_format(a.due_time,'%Y-%m') AS due_month,
  date_format(a.due_time,'%Y-%m-%d')  AS due_time,
  date_format(a.delay_due_time,'%Y-%m') AS delay_due_month,
  date_format(a.delay_due_time,'%Y-%m-%d')  AS delay_due_time,
  date_format(a.finish_time,'%Y-%m-%d') AS finish_time,
  datediff(a.finish_time,a.due_time) AS date_diff,
  datediff(a.finish_time,a.delay_due_time) AS delay_date_diff,
  case 
    when a.if_delay = 1 then '展期'
    else '未展期'end  as delay_label,
  case when a.granted_principal_amt<=5000 then '(0,5000]'
       when a.granted_principal_amt>5000 and a.granted_principal_amt<=10000 then '(5000,10000]'
       when a.granted_principal_amt>10000 and a.granted_principal_amt<=15000 then '(10000,15000]'
       when a.granted_principal_amt>15000 and a.granted_principal_amt<=20000 then '(15000,20000]'
       when a.granted_principal_amt>20000 and a.granted_principal_amt<=25000 then '(20000,25000]'
       else '(25000+)' end as amt_type_2,
  a.granted_principal_amt,   ---当期应还本金
  a.repaid_principal_amt,    ---当期已还本金
  a.interest_amt,            ---当期应还利息
  a.repaid_interest_amt,     ---当期已还利息
  a.fee_amt,                 ---当期应还服务费
  a.repaid_fee_amt,          ---当期已还服务费
  a.penalty_amt,             ---当期应还罚息
  a.repaid_penalty_amt,      ---当期已还罚息
  a.extra_amt,               ---当期应还额外收费
  a.repaid_extra_amt,        ---当期已还额外收费
  a.reduce_amt,               ---减免金额
  if(a.if_delay=0 , 0 ,b.delay_overdue_days) as delay_overdue_days,          ----首次展期时逾期天数
  sum(d.delay_amt) as delay_amt,        ---累计展期费
  MAX(d.delay_seq_cnt) as delay_seq_cnt  ----展期次数 
FROM dwb.dwb_asset_info AS a
left JOIN dim.dim_product_split as c 
  on a.product_id = c.product_id
left JOIN dm_aifox.delay_asset_detail_jf_3_overdue_days as b 
  on a.asset_item_no = b.asset_item_no
left JOIN dwb.dwb_asset_delay_info d
  on a.asset_item_no = d.asset_item_no
WHERE 
  1=1
  and a.grant_time >= '2023-01-01'
  AND a.due_time >= '2025-06-01'
group by 
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28









--------------------------各阶段未还明细(以原始账单到期日为基准)关联D-1C卡分----------------------------------



create table if not exists dm_aifox.delay_day_detial_jf_4 as
select
  a.*,
  case 
    when a.delay_label = '展期' and a.delay_seq_cnt = 1 then 'new展期'
    when a.delay_label = '展期' and a.delay_seq_cnt > 1 then 'ever展期'
    else '未展期' end as delay_label_2,
  d.max_D0_level,    ----D-1C卡分
  if(date(a.due_time) < curdate(), a.granted_principal_amt, 0) AS due_cnt,  -- 总到期本金
  if(date(a.due_time) < curdate(), a.interest_amt, 0) AS due_interest_cnt,  -- 总到期利息
  if(date(a.due_time) < curdate(), a.fee_amt, 0) AS due_fee_cnt,  -- 总到期服务费
  if(date(a.due_time) < curdate(), a.penalty_amt, 0) AS due_penalty_cnt,  -- 总到期罚息
  if(date(a.due_time) < curdate(), a.extra_amt, 0) AS due_extra_cnt,  -- 总到期额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL 3 DAY) AND (date_diff IS NULL OR date_diff > -3), a.granted_principal_amt, 0) AS D_3_overdue, -- D-3未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL 1 DAY) AND (date_diff IS NULL OR date_diff > -1), a.granted_principal_amt, 0) AS D_1_overdue, -- D-1未还本金
  if(date(a.due_time) < curdate() AND (date_diff IS NULL OR date_diff > 0), a.granted_principal_amt, 0) AS D0_overdue, -- D0未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -1 DAY) AND (date_diff IS NULL OR date_diff > 1), a.granted_principal_amt, 0) AS D1_overdue, -- D1未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff > 3), a.granted_principal_amt, 0) AS D3_overdue, -- D3未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff > 7), a.granted_principal_amt, 0) AS D7_overdue, -- D7未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff > 15), a.granted_principal_amt, 0) AS D15_overdue, -- D15未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff > 30), a.granted_principal_amt, 0) AS D30_overdue, -- D30未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff > 60), a.granted_principal_amt, 0) AS D60_overdue, -- D60未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff > 90), a.granted_principal_amt, 0) AS D90_overdue, -- D90未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -120 DAY) AND (date_diff IS NULL OR date_diff > 120), a.granted_principal_amt, 0) AS D120_overdue, -- D120未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -150 DAY) AND (date_diff IS NULL OR date_diff > 150), a.granted_principal_amt, 0) AS D150_overdue, -- D150未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -180 DAY) AND (date_diff IS NULL OR date_diff > 180), a.granted_principal_amt, 0) AS D180_overdue, -- D180未还本金
  if(date(a.due_time) < curdate(), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS due_cnt_all,  -- 总到期本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL 3 DAY) AND (date_diff IS NULL OR date_diff > -3), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D_3_overdue_all, -- D-3未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL 1 DAY) AND (date_diff IS NULL OR date_diff > -1), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D_1_overdue_all, -- D-1未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < curdate() AND (date_diff IS NULL OR date_diff > 0), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D0_overdue_all, -- D0未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -1 DAY) AND (date_diff IS NULL OR date_diff > 1), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D1_overdue_all, -- D1未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff > 3), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D3_overdue_all, -- D3未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff > 7), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D7_overdue_all, -- D7未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff > 15), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D15_overdue_all, -- D15未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff > 30), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D30_overdue_all, -- D30未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff > 60), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D60_overdue_all, -- D60未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff > 90), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D90_overdue_all, -- D90未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -120 DAY) AND (date_diff IS NULL OR date_diff > 120), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D120_overdue_all, -- D120未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -150 DAY) AND (date_diff IS NULL OR date_diff > 150), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D150_overdue_all, -- D150未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -180 DAY) AND (date_diff IS NULL OR date_diff > 180), a.granted_principal_amt+a.interest_amt+a.fee_amt+a.penalty_amt+a.extra_amt, 0) AS D180_overdue_all, -- D180未还本金+利息+服务费+罚息+额外收费
  if(date(a.due_time) < curdate(), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS due_cnt_repaid_all,  -- 总已还本金+利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL 3 DAY) AND (date_diff <= -3), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D_3_overdue_repaid_all, -- D-3已还本金+利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL 1 DAY) AND (date_diff <= -1), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D_1_overdue_repaid_all, -- D-1已还本金+利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < curdate() AND (date_diff <= 0), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D0_overdue_repaid_all, -- D0已还本金+利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -1 DAY) AND (date_diff <= 1), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D1_overdue_repaid_all, -- D1已还本金+利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff <= 3), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D3_overdue_repaid_all, -- D3已还本金+利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff <= 7), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D7_overdue_repaid_all, -- D7已还本金+利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff <= 15), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D15_overdue_repaid_all, -- D15已还本金+利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff <= 30), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D30_overdue_repaid_all, -- D30已还利本金+息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff <= 60), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D60_overdue_repaid_all, -- D60已还本金+利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff <= 90), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D90_overdue_repaid_all, -- D90已还本金+利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -120 DAY) AND (date_diff <= 120), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D120_overdue_repaid_all, -- D120已还本金+利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -150 DAY) AND (date_diff <= 150), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D150_overdue_repaid_all, -- D150已还本金+利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -180 DAY) AND (date_diff <= 180), a.repaid_principal_amt+a.repaid_interest_amt+a.repaid_fee_amt+a.repaid_penalty_amt+a.delay_amt, 0) AS D180_overdue_repaid_all -- D180已还本金+利息+服务费+罚息+额外收费+展期费  
from dm_aifox.delay_asset_detail_jf_4 as a
left join dm_aifox.d0level as d
on a.asset_item_no = d.asset_item_no
and date_add(a.delay_due_time, INTERVAL 0 DAY)=d.dt







--------------------------核心中间表(以原始账单到期日为基准)----------------------------------






create table if not exists dm_aifox.tha_delay_asset_jf_4 as

select
"金额" as stat_type,
due_month,
delay_due_month,
due_time,
delay_due_time,
user_status,
user_debt_status,
product,
apply_channel_source,
delay_label,
delay_label_2,
amt_type_2,
delay_seq_cnt,
delay_overdue_days,
max_D0_level as C_level,
sum(due_cnt) AS due_cnt,  -- 总到期本金
sum(due_interest_cnt) AS due_interest_cnt,  -- 总到期利息
sum(due_fee_cnt) AS due_fee_cnt,  -- 总到期服务费
sum(due_penalty_cnt) AS due_penalty_cnt,  -- 总到期罚息
sum(due_extra_cnt) AS due_extra_cnt,  -- 总到期额外收费
sum(D_3_overdue) AS D_3_overdue, 
sum(D_1_overdue) AS D_1_overdue, 
sum(D0_overdue) AS D0_overdue, 
sum(D1_overdue) AS D1_overdue, 
sum(D3_overdue) AS D3_overdue, 
sum(D7_overdue) AS D7_overdue, 
sum(D15_overdue) AS D15_overdue, 
sum(D30_overdue) AS D30_overdue, 
sum(D60_overdue) AS D60_overdue, 
sum(D90_overdue) AS D90_overdue, 
sum(D120_overdue) AS D120_overdue,
sum(D150_overdue) AS D150_overdue,
sum(D180_overdue) AS D180_overdue,
sum(due_cnt_all) AS due_cnt_all,  
sum(D_3_overdue_all) AS D_3_overdue_all, 
sum(D_1_overdue_all) AS D_1_overdue_all, 
sum(D0_overdue_all) AS D0_overdue_all, 
sum(D1_overdue_all) AS D1_overdue_all, 
sum(D3_overdue_all) AS D3_overdue_all, 
sum(D7_overdue_all) AS D7_overdue_all, 
sum(D15_overdue_all) AS D15_overdue_all, 
sum(D30_overdue_all) AS D30_overdue_all, 
sum(D60_overdue_all) AS D60_overdue_all, 
sum(D90_overdue_all) AS D90_overdue_all, 
sum(D120_overdue_all) AS D120_overdue_all,
sum(D150_overdue_all) AS D150_overdue_all,
sum(D180_overdue_all) AS D180_overdue_all,
sum(due_cnt_repaid_all) AS due_cnt_repaid_all,  
sum(D_3_overdue_repaid_all) AS D_3_overdue_repaid_all, 
sum(D_1_overdue_repaid_all) AS D_1_overdue_repaid_all, 
sum(D0_overdue_repaid_all) AS D0_overdue_repaid_all, 
sum(D1_overdue_repaid_all) AS D1_overdue_repaid_all, 
sum(D3_overdue_repaid_all) AS D3_overdue_repaid_all, 
sum(D7_overdue_repaid_all) AS D7_overdue_repaid_all, 
sum(D15_overdue_repaid_all) AS D15_overdue_repaid_all, 
sum(D30_overdue_repaid_all) AS D30_overdue_repaid_all, 
sum(D60_overdue_repaid_all) AS D60_overdue_repaid_all, 
sum(D90_overdue_repaid_all) AS D90_overdue_repaid_all, 
sum(D120_overdue_repaid_all) AS D120_overdue_repaid_all,
sum(D150_overdue_repaid_all) AS D150_overdue_repaid_all,
sum(D180_overdue_repaid_all) AS D180_overdue_repaid_all,
SUM(delay_amt)
from dm_aifox.delay_day_detial_jf_4
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15


union all
select
"件数" as stat_type,
due_month,
delay_due_month,
due_time,
delay_due_time,
user_status,
user_debt_status,
product,
apply_channel_source,
delay_label,
delay_label_2,
amt_type_2,
delay_seq_cnt,
delay_overdue_days,
max_D0_level as C_level,
count(distinct case when date(due_time) < curdate() then asset_item_no end ) AS due_cnt, 
null  AS due_interest_cnt,  
null  AS due_fee_cnt,  
null  AS due_penalty_cnt,  
null  AS due_extra_cnt, 
count(distinct case when D_3_overdue>0 then asset_item_no end) AS D_3_overdue, 
count(distinct case when D_1_overdue>0  then asset_item_no end) AS D_1_overdue, 
count(distinct case when D0_overdue>0  then asset_item_no end ) AS D0_overdue, 
count(distinct case when D1_overdue>0  then asset_item_no end) AS D1_overdue, 
count(distinct case when D3_overdue>0 then asset_item_no end) AS D3_overdue,
count(distinct case when D7_overdue>0 then asset_item_no end) AS D7_overdue,
count(distinct case when D15_overdue>0 then asset_item_no end) AS D15_overdue,
count(distinct case when D30_overdue>0 then asset_item_no end) AS D30_overdue,
count(distinct case when D60_overdue>0 then asset_item_no end) AS D60_overdue,
count(distinct case when D90_overdue>0 then asset_item_no end) AS D90_overdue,
count(distinct case when D120_overdue>0 then asset_item_no end) AS D120_overdue,
count(distinct case when D150_overdue>0 then asset_item_no end) AS D150_overdue,
count(distinct case when D180_overdue>0 then asset_item_no end) AS D180_overdue,
null AS due_cnt_all,  
null AS D_3_overdue_all, 
null AS D_1_overdue_all, 
null AS D0_overdue_all, 
null AS D1_overdue_all, 
null AS D3_overdue_all, 
null AS D7_overdue_all, 
null AS D15_overdue_all, 
null AS D30_overdue_all, 
null AS D60_overdue_all, 
null AS D90_overdue_all, 
null AS D120_overdue_all,
null AS D150_overdue_all,
null AS D180_overdue_all,
null AS due_cnt_repaid_all,  
null AS D_3_overdue_repaid_all, 
null AS D_1_overdue_repaid_all, 
null AS D0_overdue_repaid_all, 
null AS D1_overdue_repaid_all, 
null AS D3_overdue_repaid_all, 
null AS D7_overdue_repaid_all, 
null AS D15_overdue_repaid_all, 
null AS D30_overdue_repaid_all, 
null AS D60_overdue_repaid_all, 
null AS D90_overdue_repaid_all, 
null AS D120_overdue_repaid_all,
null AS D150_overdue_repaid_all,
null AS D180_overdue_repaid_all,
null as delay_amt
from dm_aifox.delay_day_detial_jf_4
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15



