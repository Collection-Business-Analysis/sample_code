-----------------------------建立资产维度各逾期阶段基表--------------------

drop table if exists dm_aifox.asset_day_detail_jf_wyx20260228;

create table if not exists dm_aifox.asset_day_detail_jf_wyx20260228 as 


select
   temp.*,
   if(account_overdue_date_diff_202508 > 0 , max(account_overdue_date_diff_202508) over(partition by asset_item_no ), 0) as overdue_date_diff_202508,       -----截至2025-08-01日切片资产最大逾期阶段
   if(account_overdue_date_diff_202509 > 0 , max(account_overdue_date_diff_202509) over(partition by asset_item_no ), 0) as overdue_date_diff_202509,       -----截至2025-09-01日切片资产最大逾期阶段
   if(account_overdue_date_diff_202510 > 0 , max(account_overdue_date_diff_202510) over(partition by asset_item_no ), 0) as overdue_date_diff_202510,       -----截至2025-10-01日切片资产最大逾期阶段
   if(account_overdue_date_diff_202511 > 0 , max(account_overdue_date_diff_202511) over(partition by asset_item_no ), 0) as overdue_date_diff_202511,       -----截至2025-11-01日切片资产最大逾期阶段
   if(account_overdue_date_diff_202512 > 0 , max(account_overdue_date_diff_202512) over(partition by asset_item_no ), 0) as overdue_date_diff_202512,       -----截至2025-12-01日切片资产最大逾期阶段
   if(account_overdue_date_diff_202601 > 0 , max(account_overdue_date_diff_202601) over(partition by asset_item_no ), 0) as overdue_date_diff_202601,       -----截至2026-01-01日切片资产最大逾期阶段
   if(account_overdue_date_diff_202602 > 0 , max(account_overdue_date_diff_202602) over(partition by asset_item_no ), 0) as overdue_date_diff_202602        -----截至2026-02-01日切片资产最大逾期阶段
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
    a.period_seq,   ---期次
    date_format(a.grant_time,'%Y-%m-%d') AS grant_time,      ---借款日期
    datediff(a.finish_time,a.delay_due_time) AS date_diff,   ---账单结清日期差
    date_format(a.delay_due_time,'%Y-%m') AS delay_due_month,   ---逾期日期月份
    date_format(a.delay_due_time,'%Y-%m-%d')  AS delay_due_time,  ---逾期日期
    date_format(a.finish_time,'%Y-%m-%d') AS finish_time,       ----结清日期
    a.asset_overdue_period_days,           
    if(a.delay_due_time < '2025-08-01' and (a.finish_time is null or a.finish_time >= '2025-08-01') ,datediff('2025-08-01',a.delay_due_time),0)  AS account_overdue_date_diff_202508,  -----截至2025-08-01日切片资产各账单逾期阶段
    if(a.delay_due_time < '2025-09-01' and (a.finish_time is null or a.finish_time >= '2025-09-01') ,datediff('2025-09-01',a.delay_due_time),0)  AS account_overdue_date_diff_202509,  -----截至2025-09-01日切片资产各账单逾期阶段
    if(a.delay_due_time < '2025-10-01' and (a.finish_time is null or a.finish_time >= '2025-10-01') ,datediff('2025-10-01',a.delay_due_time),0)  AS account_overdue_date_diff_202510,  -----截至2025-10-01日切片资产各账单逾期阶段
    if(a.delay_due_time < '2025-11-01' and (a.finish_time is null or a.finish_time >= '2025-11-01') ,datediff('2025-11-01',a.delay_due_time),0)  AS account_overdue_date_diff_202511,  -----截至2025-11-01日切片资产各账单逾期阶段
    if(a.delay_due_time < '2025-12-01' and (a.finish_time is null or a.finish_time >= '2025-12-01') ,datediff('2025-12-01',a.delay_due_time),0)  AS account_overdue_date_diff_202512,  -----截至2025-12-01日切片资产各账单逾期阶段
    if(a.delay_due_time < '2026-01-01' and (a.finish_time is null or a.finish_time >= '2026-01-01') ,datediff('2026-01-01',a.delay_due_time),0)  AS account_overdue_date_diff_202601,  -----截至2026-01-01日切片资产各账单逾期阶段
    if(a.delay_due_time < '2026-02-01' and (a.finish_time is null or a.finish_time >= '2026-02-01') ,datediff('2026-02-01',a.delay_due_time),0)  AS account_overdue_date_diff_202602,  -----截至2026-02-01日切片资产各账单逾期阶段
    a.granted_principal_period_amt,       --应还本金
    a.repaid_principal_period_amt         --还款本金
  FROM 
    dwb.dwb_asset_period_info AS a
  INNER JOIN  dwb.dwb_asset_info AS c ON a.asset_item_no = c.asset_item_no
  left JOIN dim.dim_product_split d on a.product_id = d.product_id
  WHERE 
    1=1
    and a.grant_time >= '2023-01-01'
  ) temp





-----------------每月首日日期切片判断资产逾期各阶段的未还情况------------逾期90+细拆----------
(

select
  '印尼' as '国家',
  '2025-08' as '月份',
  -- case 
  --   when asset_overdue_period_days in (0,1) then '新增资产'
  --   else '非新增资产'
  -- end as '是否新增资产',
  case 
    when overdue_date_diff_202509 <=0 then "未逾期"
    when overdue_date_diff_202509 >0 and overdue_date_diff_202509 <= 90 then "1-90"
    when overdue_date_diff_202509 >90 and overdue_date_diff_202509<=180  then "90-180"
    when overdue_date_diff_202509 >180 and overdue_date_diff_202509<=360 then "180-360"
    when overdue_date_diff_202509 >360  then "360+"
  end as '逾期阶段',
  count(distinct case when date(delay_due_time) < '2025-09-01' then asset_item_no end ) as due_cnt,  -- 总到期件数
  SUM(if(date(delay_due_time) < '2025-09-01', granted_principal_period_amt, 0)) AS due_amount, -- 总到期本金
  SUM(if(date(delay_due_time) < '2025-09-01' AND (datediff(finish_time,'2025-09-01') IS NULL OR datediff(finish_time,'2025-09-01') > 7), granted_principal_period_amt, 0)) AS D7_overdue, -- D7未还金额
  SUM(if(date(delay_due_time) < '2025-09-01' AND (datediff(finish_time,'2025-09-01') IS NULL OR datediff(finish_time,'2025-09-01') > 15), granted_principal_period_amt, 0)) AS D15_overdue, -- D15未还金额
  SUM(if(date(delay_due_time) < '2025-09-01' AND (datediff(finish_time,'2025-09-01') IS NULL OR datediff(finish_time,'2025-09-01') > 30), granted_principal_period_amt, 0)) AS D30_overdue -- D30未还金额
FROM dm_aifox.asset_day_detail_jf_wyx20260228
group by 1,2,3


union all


select
  '印尼' as '国家',
  '2025-09' as '月份',
  -- case 
  --   when asset_overdue_period_days in (0,1) then '新增资产'
  --   else '非新增资产'
  -- end as '是否新增资产',
  case 
    when overdue_date_diff_202510 <=0 then "未逾期"
    when overdue_date_diff_202510 >0 and overdue_date_diff_202510 <= 90 then "1-90"
    when overdue_date_diff_202510 >90 and overdue_date_diff_202510<=180  then "90-180"
    when overdue_date_diff_202510 >180 and overdue_date_diff_202510<=360 then "180-360"
    when overdue_date_diff_202510 >360  then "360+"
  end as '逾期阶段',
  count(distinct case when date(delay_due_time) < '2025-10-01' then asset_item_no end ) as due_cnt,  -- 总到期件数
  SUM(if(date(delay_due_time) < '2025-10-01', granted_principal_period_amt, 0)) AS due_amount, -- 总到期本金
  SUM(if(date(delay_due_time) < '2025-10-01' AND (datediff(finish_time,'2025-10-01') IS NULL OR datediff(finish_time,'2025-10-01') > 7), granted_principal_period_amt, 0)) AS D7_overdue, -- D7未还金额
  SUM(if(date(delay_due_time) < '2025-10-01' AND (datediff(finish_time,'2025-10-01') IS NULL OR datediff(finish_time,'2025-10-01') > 15), granted_principal_period_amt, 0)) AS D15_overdue, -- D15未还金额
  SUM(if(date(delay_due_time) < '2025-10-01' AND (datediff(finish_time,'2025-10-01') IS NULL OR datediff(finish_time,'2025-10-01') > 30), granted_principal_period_amt, 0)) AS D30_overdue -- D30未还金额
FROM dm_aifox.asset_day_detail_jf_wyx20260228
group by 1,2,3


union all


select
  '印尼' as '国家',
  '2025-10' as '月份',
  -- case 
  --   when asset_overdue_period_days in (0,1) then '新增资产'
  --   else '非新增资产'
  -- end as '是否新增资产',
  case 
    when overdue_date_diff_202511 <=0 then "未逾期"
    when overdue_date_diff_202511 >0 and overdue_date_diff_202511 <= 90 then "1-90"
    when overdue_date_diff_202511 >90 and overdue_date_diff_202511<=180  then "90-180"
    when overdue_date_diff_202511 >180 and overdue_date_diff_202511<=360 then "180-360"
    when overdue_date_diff_202511 >360  then "360+"
  end as '逾期阶段',
  count(distinct case when date(delay_due_time) < '2025-11-01' then asset_item_no end ) as due_cnt,  -- 总到期件数
  SUM(if(date(delay_due_time) < '2025-11-01', granted_principal_period_amt, 0)) AS due_amount, -- 总到期本金
  SUM(if(date(delay_due_time) < '2025-11-01' AND (datediff(finish_time,'2025-11-01') IS NULL OR datediff(finish_time,'2025-11-01') > 7), granted_principal_period_amt, 0)) AS D7_overdue, -- D7未还金额
  SUM(if(date(delay_due_time) < '2025-11-01' AND (datediff(finish_time,'2025-11-01') IS NULL OR datediff(finish_time,'2025-11-01') > 15), granted_principal_period_amt, 0)) AS D15_overdue, -- D15未还金额
  SUM(if(date(delay_due_time) < '2025-11-01' AND (datediff(finish_time,'2025-11-01') IS NULL OR datediff(finish_time,'2025-11-01') > 30), granted_principal_period_amt, 0)) AS D30_overdue -- D30未还金额
FROM dm_aifox.asset_day_detail_jf_wyx20260228
group by 1,2,3




union all


select
  '印尼' as '国家',
  '2025-11' as '月份',
  -- case 
  --   when asset_overdue_period_days in (0,1) then '新增资产'
  --   else '非新增资产'
  -- end as '是否新增资产',
  case 
    when overdue_date_diff_202512 <=0 then "未逾期"
    when overdue_date_diff_202512 >0 and overdue_date_diff_202512 <= 90 then "1-90"
    when overdue_date_diff_202512 >90 and overdue_date_diff_202512<=180  then "90-180"
    when overdue_date_diff_202512 >180 and overdue_date_diff_202512<=360 then "180-360"
    when overdue_date_diff_202512 >360  then "360+"
  end as '逾期阶段',
  count(distinct case when date(delay_due_time) < '2025-12-01' then asset_item_no end ) as due_cnt,  -- 总到期件数
  SUM(if(date(delay_due_time) < '2025-12-01', granted_principal_period_amt, 0)) AS due_amount, -- 总到期本金
  SUM(if(date(delay_due_time) < '2025-12-01' AND (datediff(finish_time,'2025-12-01') IS NULL OR datediff(finish_time,'2025-12-01') > 7), granted_principal_period_amt, 0)) AS D7_overdue, -- D7未还金额
  SUM(if(date(delay_due_time) < '2025-12-01' AND (datediff(finish_time,'2025-12-01') IS NULL OR datediff(finish_time,'2025-12-01') > 15), granted_principal_period_amt, 0)) AS D15_overdue, -- D15未还金额
  SUM(if(date(delay_due_time) < '2025-12-01' AND (datediff(finish_time,'2025-12-01') IS NULL OR datediff(finish_time,'2025-12-01') > 30), granted_principal_period_amt, 0)) AS D30_overdue -- D30未还金额
FROM dm_aifox.asset_day_detail_jf_wyx20260228
group by 1,2,3



union all


select
  '印尼' as '国家',
  '2025-12' as '月份',
  -- case 
  --   when asset_overdue_period_days in (0,1) then '新增资产'
  --   else '非新增资产'
  -- end as '是否新增资产',
  case 
    when overdue_date_diff_202601 <=0 then "未逾期"
    when overdue_date_diff_202601 >0 and overdue_date_diff_202601 <= 90 then "1-90"
    when overdue_date_diff_202601 >90 and overdue_date_diff_202601<=180  then "90-180"
    when overdue_date_diff_202601 >180 and overdue_date_diff_202601<=360 then "180-360"
    when overdue_date_diff_202601 >360  then "360+"
  end as '逾期阶段',
  count(distinct case when date(delay_due_time) < '2026-01-01' then asset_item_no end ) as due_cnt,  -- 总到期件数
  SUM(if(date(delay_due_time) < '2026-01-01', granted_principal_period_amt, 0)) AS due_amount, -- 总到期本金
  SUM(if(date(delay_due_time) < '2026-01-01' AND (datediff(finish_time,'2026-01-01') is null or datediff(finish_time,'2026-01-01') > 7), granted_principal_period_amt, 0)) AS D7_overdue, -- D7未还金额
  SUM(if(date(delay_due_time) < '2026-01-01' AND (datediff(finish_time,'2026-01-01') is null or datediff(finish_time,'2026-01-01') > 15), granted_principal_period_amt, 0)) AS D15_overdue, -- D15未还金额
  SUM(if(date(delay_due_time) < '2026-01-01' AND (datediff(finish_time,'2026-01-01') is null or datediff(finish_time,'2026-01-01') > 30), granted_principal_period_amt, 0)) AS D30_overdue -- D30未还金额
FROM dm_aifox.asset_day_detail_jf_wyx20260228
group by 1,2,3



union all


select
  '印尼' as '国家',
  '2026-01' as '月份',
  -- case 
  --   when asset_overdue_period_days in (0,1) then '新增资产'
  --   else '非新增资产'
  -- end as '是否新增资产',
  case 
    when overdue_date_diff_202602 <=0 then "未逾期"
    when overdue_date_diff_202602 >0 and overdue_date_diff_202602 <= 90 then "1-90"
    when overdue_date_diff_202602 >90 and overdue_date_diff_202602<=180  then "90-180"
    when overdue_date_diff_202602 >180 and overdue_date_diff_202602<=360 then "180-360"
    when overdue_date_diff_202602 >360  then "360+"
  end as '逾期阶段',
  count(distinct case when date(delay_due_time) < '2026-02-01' then asset_item_no end ) as due_cnt,  -- 总到期件数
  SUM(if(date(delay_due_time) < '2026-02-01', granted_principal_period_amt, 0)) AS due_amount, -- 总到期本金
  SUM(if(date(delay_due_time) < '2026-02-01' AND (datediff(finish_time,'2026-02-01') is null or datediff(finish_time,'2026-02-01') > 7), granted_principal_period_amt, 0)) AS D7_overdue, -- D7未还金额
  SUM(if(date(delay_due_time) < '2026-02-01' AND (datediff(finish_time,'2026-02-01') is null or datediff(finish_time,'2026-02-01') > 15), granted_principal_period_amt, 0)) AS D15_overdue, -- D15未还金额
  SUM(if(date(delay_due_time) < '2026-02-01' AND (datediff(finish_time,'2026-02-01') is null or datediff(finish_time,'2026-02-01') > 30), granted_principal_period_amt, 0)) AS D30_overdue -- D30未还金额
FROM dm_aifox.asset_day_detail_jf_wyx20260228
group by 1,2,3

)
order by 1,2,3









-----------------每月首日日期切片判断资产逾期各阶段的未还情况------------全量资产----------
(
select
  '印尼' as '国家',
  '2025-08' as '月份',
  -- case 
  --   when asset_overdue_period_days in (0,1) then '新增资产'
  --   else '非新增资产'
  -- end as '是否新增资产',
  case 
    when overdue_date_diff_202509 <=0 then "未逾期"
    when overdue_date_diff_202509 >0 and overdue_date_diff_202509 <= 90 then "1-90"
    when overdue_date_diff_202509 >90  then "90+"
  end as '逾期阶段',
  count(distinct case when date(delay_due_time) < '2025-09-01' then asset_item_no end ) as due_cnt,  -- 总到期件数
  SUM(if(date(delay_due_time) < '2025-09-01', granted_principal_period_amt, 0)) AS due_amount, -- 总到期本金
  SUM(if(date(delay_due_time) < '2025-09-01' AND (datediff(finish_time,'2025-09-01') IS NULL OR datediff(finish_time,'2025-09-01') > 7), granted_principal_period_amt, 0)) AS D7_overdue, -- D7未还金额
  SUM(if(date(delay_due_time) < '2025-09-01' AND (datediff(finish_time,'2025-09-01') IS NULL OR datediff(finish_time,'2025-09-01') > 15), granted_principal_period_amt, 0)) AS D15_overdue, -- D15未还金额
  SUM(if(date(delay_due_time) < '2025-09-01' AND (datediff(finish_time,'2025-09-01') IS NULL OR datediff(finish_time,'2025-09-01') > 30), granted_principal_period_amt, 0)) AS D30_overdue -- D30未还金额
FROM dm_aifox.asset_day_detail_jf_wyx20260228
group by 1,2,3


union all


select
  '印尼' as '国家',
  '2025-09' as '月份',
  -- case 
  --   when asset_overdue_period_days in (0,1) then '新增资产'
  --   else '非新增资产'
  -- end as '是否新增资产',
  case 
    when overdue_date_diff_202510 <=0 then "未逾期"
    when overdue_date_diff_202510 >0 and overdue_date_diff_202510 <= 90 then "1-90"
    when overdue_date_diff_202510 >90  then "90+"
  end as '逾期阶段',
  count(distinct case when date(delay_due_time) < '2025-10-01' then asset_item_no end ) as due_cnt,  -- 总到期件数
  SUM(if(date(delay_due_time) < '2025-10-01', granted_principal_period_amt, 0)) AS due_amount, -- 总到期本金
  SUM(if(date(delay_due_time) < '2025-10-01' AND (datediff(finish_time,'2025-10-01') IS NULL OR datediff(finish_time,'2025-10-01') > 7), granted_principal_period_amt, 0)) AS D7_overdue, -- D7未还金额
  SUM(if(date(delay_due_time) < '2025-10-01' AND (datediff(finish_time,'2025-10-01') IS NULL OR datediff(finish_time,'2025-10-01') > 15), granted_principal_period_amt, 0)) AS D15_overdue, -- D15未还金额
  SUM(if(date(delay_due_time) < '2025-10-01' AND (datediff(finish_time,'2025-10-01') IS NULL OR datediff(finish_time,'2025-10-01') > 30), granted_principal_period_amt, 0)) AS D30_overdue -- D30未还金额
FROM dm_aifox.asset_day_detail_jf_wyx20260228
group by 1,2,3


union all


select
  '印尼' as '国家',
  '2025-10' as '月份',
  -- case 
  --   when asset_overdue_period_days in (0,1) then '新增资产'
  --   else '非新增资产'
  -- end as '是否新增资产',
  case 
    when overdue_date_diff_202511 <=0 then "未逾期"
    when overdue_date_diff_202511 >0 and overdue_date_diff_202511 <= 90 then "1-90"
    when overdue_date_diff_202511 >90  then "90+"
  end as '逾期阶段',
  count(distinct case when date(delay_due_time) < '2025-11-01' then asset_item_no end ) as due_cnt,  -- 总到期件数
  SUM(if(date(delay_due_time) < '2025-11-01', granted_principal_period_amt, 0)) AS due_amount, -- 总到期本金
  SUM(if(date(delay_due_time) < '2025-11-01' AND (datediff(finish_time,'2025-11-01') IS NULL OR datediff(finish_time,'2025-11-01') > 7), granted_principal_period_amt, 0)) AS D7_overdue, -- D7未还金额
  SUM(if(date(delay_due_time) < '2025-11-01' AND (datediff(finish_time,'2025-11-01') IS NULL OR datediff(finish_time,'2025-11-01') > 15), granted_principal_period_amt, 0)) AS D15_overdue, -- D15未还金额
  SUM(if(date(delay_due_time) < '2025-11-01' AND (datediff(finish_time,'2025-11-01') IS NULL OR datediff(finish_time,'2025-11-01') > 30), granted_principal_period_amt, 0)) AS D30_overdue -- D30未还金额
FROM dm_aifox.asset_day_detail_jf_wyx20260228
group by 1,2,3




union all


select
  '印尼' as '国家',
  '2025-11' as '月份',
  -- case 
  --   when asset_overdue_period_days in (0,1) then '新增资产'
  --   else '非新增资产'
  -- end as '是否新增资产',
  case 
    when overdue_date_diff_202512 <=0 then "未逾期"
    when overdue_date_diff_202512 >0 and overdue_date_diff_202512 <= 90 then "1-90"
    when overdue_date_diff_202512 >90  then "90+"
  end as '逾期阶段',
  count(distinct case when date(delay_due_time) < '2025-12-01' then asset_item_no end ) as due_cnt,  -- 总到期件数
  SUM(if(date(delay_due_time) < '2025-12-01', granted_principal_period_amt, 0)) AS due_amount, -- 总到期本金
  SUM(if(date(delay_due_time) < '2025-12-01' AND (datediff(finish_time,'2025-12-01') IS NULL OR datediff(finish_time,'2025-12-01') > 7), granted_principal_period_amt, 0)) AS D7_overdue, -- D7未还金额
  SUM(if(date(delay_due_time) < '2025-12-01' AND (datediff(finish_time,'2025-12-01') IS NULL OR datediff(finish_time,'2025-12-01') > 15), granted_principal_period_amt, 0)) AS D15_overdue, -- D15未还金额
  SUM(if(date(delay_due_time) < '2025-12-01' AND (datediff(finish_time,'2025-12-01') IS NULL OR datediff(finish_time,'2025-12-01') > 30), granted_principal_period_amt, 0)) AS D30_overdue -- D30未还金额
FROM dm_aifox.asset_day_detail_jf_wyx20260228
group by 1,2,3



union all


select
  '印尼' as '国家',
  '2025-12' as '月份',
  -- case 
  --   when asset_overdue_period_days in (0,1) then '新增资产'
  --   else '非新增资产'
  -- end as '是否新增资产',
  case 
    when overdue_date_diff_202601 <=0 then "未逾期"
    when overdue_date_diff_202601 >0 and overdue_date_diff_202601 <= 90 then "1-90"
    when overdue_date_diff_202601 >90  then "90+"
  end as '逾期阶段',
  count(distinct case when date(delay_due_time) < '2026-01-01' then asset_item_no end ) as due_cnt,  -- 总到期件数
  SUM(if(date(delay_due_time) < '2026-01-01', granted_principal_period_amt, 0)) AS due_amount, -- 总到期本金
  SUM(if(date(delay_due_time) < '2026-01-01' AND (datediff(finish_time,'2026-01-01') is null or datediff(finish_time,'2026-01-01') > 7), granted_principal_period_amt, 0)) AS D7_overdue, -- D7未还金额
  SUM(if(date(delay_due_time) < '2026-01-01' AND (datediff(finish_time,'2026-01-01') is null or datediff(finish_time,'2026-01-01') > 15), granted_principal_period_amt, 0)) AS D15_overdue, -- D15未还金额
  SUM(if(date(delay_due_time) < '2026-01-01' AND (datediff(finish_time,'2026-01-01') is null or datediff(finish_time,'2026-01-01') > 30), granted_principal_period_amt, 0)) AS D30_overdue -- D30未还金额
FROM dm_aifox.asset_day_detail_jf_wyx20260228
group by 1,2,3


union all


select
  '印尼' as '国家',
  '2026-01' as '月份',
  -- case 
  --   when asset_overdue_period_days in (0,1) then '新增资产'
  --   else '非新增资产'
  -- end as '是否新增资产',
  case 
    when overdue_date_diff_202602 <=0 then "未逾期"
    when overdue_date_diff_202602 >0 and overdue_date_diff_202602 <= 90 then "1-90"
    when overdue_date_diff_202602 >90  then "90+"
  end as '逾期阶段',
  count(distinct case when date(delay_due_time) < '2026-02-01' then asset_item_no end ) as due_cnt,  -- 总到期件数
  SUM(if(date(delay_due_time) < '2026-02-01', granted_principal_period_amt, 0)) AS due_amount, -- 总到期本金
  SUM(if(date(delay_due_time) < '2026-02-01' AND (datediff(finish_time,'2026-02-01') is null or datediff(finish_time,'2026-02-01') > 7), granted_principal_period_amt, 0)) AS D7_overdue, -- D7未还金额
  SUM(if(date(delay_due_time) < '2026-02-01' AND (datediff(finish_time,'2026-02-01') is null or datediff(finish_time,'2026-02-01') > 15), granted_principal_period_amt, 0)) AS D15_overdue, -- D15未还金额
  SUM(if(date(delay_due_time) < '2026-02-01' AND (datediff(finish_time,'2026-02-01') is null or datediff(finish_time,'2026-02-01') > 30), granted_principal_period_amt, 0)) AS D30_overdue -- D30未还金额
FROM dm_aifox.asset_day_detail_jf_wyx20260228
group by 1,2,3

)
order by 1,2,3




   

-----------验数-----------------

select
  *
FROM dm_aifox.asset_day_detail_jf_wyx20260228
where asset_item_no ='E2506194489348149881'














