
-----------------------------建立资产维度各逾期阶段基表--------------------

drop table if exists dm_aifox.asset_day_detail_jf_wyx20260225;

create table if not exists dm_aifox.asset_day_detail_jf_wyx20260225 as 
SELECT
  temp.*,
  if(date(temp.delay_due_time) < curdate(), temp.granted_principal_period_amt, 0) AS due_amount,  -- 总到期本金
  if(date(temp.delay_due_time) < date_add(curdate(), INTERVAL 7 DAY) AND (date_diff IS NULL OR date_diff > -7), temp.granted_principal_period_amt, 0) AS D_7_overdue, -- D-7未还金额
  if(date(temp.delay_due_time) < date_add(curdate(), INTERVAL 3 DAY) AND (date_diff IS NULL OR date_diff > -3), temp.granted_principal_period_amt, 0) AS D_3_overdue, -- D-3未还金额
  if(date(temp.delay_due_time) < date_add(curdate(), INTERVAL 1 DAY) AND (date_diff IS NULL OR date_diff > -1), temp.granted_principal_period_amt, 0) AS D_1_overdue, -- D-1未还金额
  if(date(temp.delay_due_time) < curdate() AND (date_diff IS NULL OR date_diff > 0), temp.granted_principal_period_amt, 0) AS D0_overdue, -- D0未还金额
  if(date(temp.delay_due_time) < date_add(curdate(), INTERVAL -1 DAY) AND (date_diff IS NULL OR date_diff > 1), temp.granted_principal_period_amt, 0) AS D1_overdue, -- D1未还金额
  if(date(temp.delay_due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff > 3), temp.granted_principal_period_amt, 0) AS D3_overdue, -- D3未还金额
  if(date(temp.delay_due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff > 7), temp.granted_principal_period_amt, 0) AS D7_overdue, -- D7未还金额
  if(date(temp.delay_due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff > 15), temp.granted_principal_period_amt, 0) AS D15_overdue, -- D15未还金额
  if(date(temp.delay_due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff > 30), temp.granted_principal_period_amt, 0) AS D30_overdue, -- D30未还金额
  if(date(temp.delay_due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff > 60), temp.granted_principal_period_amt, 0) AS D60_overdue, -- D60未还金额
  if(date(temp.delay_due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff > 90), temp.granted_principal_period_amt, 0) AS D90_overdue -- D90未还金额
FROM
  (
    select 
      a.asset_item_no,   ---资产编号
      a.user_id,    ---用户编号
      if(a.user_debt_status='new_user','new','old') AS user_status, ---首续贷
      a.user_debt_status,  --用户类型
      a.apply_channel_source,   ---产品包
      c.asset_loan_channel,
      c.product_form,
      c.product_period_unit,
      d.product_period as product,
      -- concat(c.product_period_qty,'天','*',c.product_period_cnt,'期') as product,  ---产品类型
      a.period_seq,   ---期次
      date_format(a.grant_time,'%Y-%m-%d') AS grant_time,      ---借款日期
      datediff(a.finish_time,a.delay_due_time) AS date_diff,   ---账单结清日期差
      date_format(a.delay_due_time,'%Y-%m') AS delay_due_month,   ---逾期日期月份
      date_format(a.delay_due_time,'%Y-%m-%d')  AS delay_due_time,  ---逾期日期
      date_format(a.finish_time,'%Y-%m-%d') AS finish_time,       ----结清日期
      a.asset_overdue_period_days,           --资产逾期阶段
      a.granted_principal_period_amt,       --应还本金
      a.repaid_principal_period_amt         --还款本金
    FROM 
      dwb.dwb_asset_period_info AS a
    INNER JOIN  dwb.dwb_asset_info AS c ON a.asset_item_no = c.asset_item_no
    left JOIN dim.dim_product_split d on a.product_id = d.product_id
    WHERE 
      1=1
      -- and a.asset_overdue_period_days in (0,1)            --限定新增资产
      and a.grant_time >= '2023-01-01'
      AND a.delay_due_time >= '2025-07-01'
  ) temp
WHERE 
 temp.delay_due_time >= '2025-08-01'
  AND temp.delay_due_time <= '2026-01-31'





-----------------资产逾期各阶段的未还情况----------------------

SELECT
  '泰国' as '国家',
  delay_due_month,           ---资产逾期月份
  user_status,               ---首续贷
  product,                   ---产品
  null as period_seq,                ---产品期次
  null as apply_channel_source,      ---产品包
  case 
    when asset_overdue_period_days in (0,1) then '新增资产'
    when asset_overdue_period_days > 90 then '资产逾期90+'
    when asset_overdue_period_days > 60 and asset_overdue_period_days <= 90 then '资产逾期61~90天'
    when asset_overdue_period_days > 30 and asset_overdue_period_days <= 60 then '资产逾期31~60天'
    when asset_overdue_period_days > 15 and asset_overdue_period_days <= 30 then '资产逾期16~30天'
    when asset_overdue_period_days > 7 and asset_overdue_period_days <= 15 then '资产逾期8~15天'
    when asset_overdue_period_days > 1 and asset_overdue_period_days <= 7 then '资产逾期2~7天'
    when asset_overdue_period_days is null then '资产未到期'
  else 'Error' end as asset_overdue_type,           --资产逾期阶段
  sum(due_amount) AS due_amount,  
  sum(D_7_overdue) AS D_7_overdue_amount, 
  sum(D_3_overdue) AS D_3_overdue_amount, 
  sum(D_1_overdue) AS D_1_overdue_amount, 
  sum(D0_overdue) AS D0_overdue_amount, 
  sum(D1_overdue) AS D1_overdue_amount, 
  sum(D3_overdue) AS D3_overdue_amount, 
  sum(D7_overdue) AS D7_overdue_amount, 
  sum(D15_overdue) AS D15_overdue_amount, 
  sum(D30_overdue) AS D30_overdue_amount, 
  sum(D60_overdue) AS D60_overdue_amount, 
  sum(D90_overdue) AS D90_overdue_amount,
  count(distinct case when date(delay_due_time) < curdate() then asset_item_no end ) AS due_cnt,  
  count(distinct case when D_7_overdue>0  then asset_item_no end) AS D_7_overdue_cnt, 
  count(distinct case when D_3_overdue>0 then asset_item_no end) AS D_3_overdue_cnt, 
  count(distinct case when D_1_overdue>0  then asset_item_no end) AS D_1_overdue_cnt, 
  count(distinct case when D0_overdue>0  then asset_item_no end ) AS D0_overdue_cnt, 
  count(distinct case when D1_overdue>0  then asset_item_no end) AS D1_overdue_cnt, 
  count(distinct case when D3_overdue>0 then asset_item_no end) AS D3_overdue_cnt,
  count(distinct case when D7_overdue>0 then asset_item_no end) AS D7_overdue_cnt,
  count(distinct case when D15_overdue>0 then asset_item_no end) AS D15_overdue_cnt,
  count(distinct case when D30_overdue>0 then asset_item_no end) AS D30_overdue_cnt,
  count(distinct case when D60_overdue>0 then asset_item_no end) AS D60_overdue_cnt,
  count(distinct case when D90_overdue>0 then asset_item_no end) AS D90_overdue_cnt
FROM
  dm_aifox.asset_day_detail_jf_wyx20260225
GROUP BY
  1,2,3,4,5,6,7
ORDER BY
  1,2,3,4,5,6,7
