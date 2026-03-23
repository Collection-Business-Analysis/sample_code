
-------单笔资产多次展期实例----------

-------历史用户可选展期天数，自2025年9月开始按产品期数展-------

SELECT 
  *
FROM
  dwb.dwb_asset_delay_info a
-- left JOIN dim.dim_product_split d on
-- a.product_id = d.product_id
where 
-- asset_item_no ='C2025082420735224008'
  -- asset_item_no = 'T2023072847386351920'
-- asset_item_no = 'T2024100858456332104'
asset_item_no='T2025081378097001484'


-------同一债务人多笔资产展期实例-------

-------同一人多笔资产逾期，展期仅针对单一资产下所有期次，而非债务人名下所有资产-------


SELECT 
  *
FROM
  dwb.dwb_asset_delay_period_info a
-- left JOIN dim.dim_product_split d on
-- a.product_id = d.product_id
where 
  user_id = '10019055'
  -- user_debt_status = 'old_debt'



--------------每次展期时距离还款日的时间差-----------------

SELECT 
    asset_item_no,
    period_seq,
    DATEDIFF(LEAD(delay_pay_time, 1, NULL) OVER (PARTITION BY asset_item_no,period_seq ORDER BY delay_seq_cnt),delay_due_time) AS delay_overdue_days
FROM
    dwb.dwb_asset_delay_period_info 
where 
  asset_item_no = 'T2025080376104260369'






-------------------------------------------------

-- dwb.dwb_asset_info
-- dwb.dwb_asset_period_info
-- dwd.dwd_asset_delay
-- dwb.dwb_asset_delay_info
-- dwb.dwb_asset_delay_period_info










--------------------------------------------------------------------------------
drop table if exists dm_aifox.d_1level;
drop table if exists dm_aifox.delay_asset_detail_jf_3_overdue_days;

drop table if exists dm_aifox.delay_asset_detail_jf_3;
drop table if exists dm_aifox.delay_day_detial_jf_3 ;
drop table if exists dm_aifox.tha_delay_asset_jf_3 



---------------展期资产关联展期逾期天数----------------------------


create table if not exists dm_aifox.delay_order_asset_detail_jf_2 as 
select
	a.asset_item_no as '订单编号',
	date_format(a.due_time,'%Y-%m') as '还款月',
	CASE 
		WHEN a.from_system='tha072' then 'tha072'
		WHEN a.from_system='tha073' then 'tha073'
		ELSE '其他包' END as '包名',
	CASE
		WHEN b.user_debt_status='new_user' then '首贷'
		ELSE '续贷' END as '首续贷',
	d.product_period as product,
	CASE
	  when c.delay_overdue_days = -1 then 'D-1'
	  when c.delay_overdue_days = 0 then 'D0'
	  when c.delay_overdue_days = 1 then 'D1'
	  ELSE 'D2+'
	END as '首次展期逾期阶段',
	max(a.delay_seq_cnt) as '展期次数',
	sum(a.delay_amt) as '累积展期金额',
	max(b.granted_principal_amt) as '放款本金',
	max(b.repaid_principal_amt) as '已还本金',
	max(b.interest_amt) as '利息总额',
	max(b.repaid_interest_amt) as '已还利息总额',
	max(b.fee_amt) as '服务费总额',
	max(b.repaid_fee_amt) as '已还服务费总额',
	max(b.extra_amt) as '额外收费',
	max(b.repaid_extra_amt) as '已还额外收费',
	max(b.penalty_amt) as '违约金总额',
	max(b.repaid_penalty_amt) as '已还违约金总额',
	max(b.reduce_amt) as '减免金额',
	max(a.user_uuid) as user_uuid
from dwb.dwb_asset_delay_info as a
join dwb.dwb_asset_info as b 
  on a.asset_item_no=b.asset_item_no
join dm_aifox.delay_asset_detail_jf_2_overdue_days as c 
  on a.asset_item_no=c.asset_item_no
left JOIN dim.dim_product_split as d
  on a.product_id = d.product_id
where 1=1
AND a.delay_pay_time >= '2025-06-01'
AND a.delay_pay_time < '2025-12-01'
AND a.due_time < '2025-12-01'
group by 1,2,3,4,5,6





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



create table if not exists dm_aifox.delay_asset_detail_jf_3 as 
select 
  a.asset_item_no,
  a.user_id,
  if(a.user_debt_status='new_user','new','old') AS user_status,
  a.user_debt_status,
  a.apply_channel_source,
  a.period_seq,
  c.product_period as product,
  date_format(a.grant_time,'%Y-%m-%d') AS grant_time,
  date_format(a.due_time,'%Y-%m') AS due_month,
  date_format(a.due_time,'%Y-%m-%d')  AS due_time,
  date_format(a.delay_due_time,'%Y-%m') AS delay_due_month,
  date_format(a.delay_due_time,'%Y-%m-%d')  AS delay_due_time,
  date_format(a.finish_time,'%Y-%m-%d') AS finish_time,
  datediff(a.finish_time,a.due_time) AS date_diff,
  datediff(a.finish_time,a.delay_due_time) AS delay_date_diff,
  case when a.asset_overdue_period_days in (0,1) then '是'
       when a.asset_overdue_period_days is null then '未到期'
       else '否' end asset_new_due,
  case when a.overdue_period_days in (0,1) then '是'
       when a.overdue_period_days is null then '未到期'
       else '否' end debtor_new_due,
  case when a.due_time = a.delay_due_time  then '未展期'
       else '展期' end as delay_label,
  case when a.granted_principal_period_amt<=1000 then '(0,1000]'
       when a.granted_principal_period_amt>1000 and a.granted_principal_period_amt<=2000 then '(1000,2000]'
       when a.granted_principal_period_amt>2000 and a.granted_principal_period_amt<=4000 then '(2000,4000]'
       when a.granted_principal_period_amt>4000 and a.granted_principal_period_amt<=5000 then '(4000,5000]'
       else '(5000+)' end as amt_type_2,
  a.granted_principal_period_amt,   ---当期应还本金
  a.repaid_principal_period_amt,    ---当期已还本金
  a.interest_period_amt,            ---当期应还利息
  a.repaid_interest_period_amt,     ---当期已还利息
  a.fee_period_amt,                 ---当期应还服务费
  a.repaid_fee_period_amt,          ---当期已还服务费
  a.penalty_period_amt,             ---当期应还罚息
  a.repaid_penalty_period_amt,      ---当期已还罚息
  a.extra_period_amt,               ---当期应还额外收费
  a.repaid_extra_period_amt,        ---当期已还额外收费
  a.repaid_delay_period_amt,        ---累计展期费
  a.reduce_period_amt,               ---减免金额
  if(a.due_time = a.delay_due_time , 0 ,b.delay_overdue_days) as delay_overdue_days,          ----首次展期时逾期天数
  MAX(d.delay_seq_cnt) as delay_seq_cnt  ----展期次数 
FROM dwb.dwb_asset_period_info AS a
left JOIN dim.dim_product_split as c 
  on a.product_id = c.product_id
left JOIN dm_aifox.delay_asset_detail_jf_3_overdue_days as b 
  on a.asset_item_no = b.asset_item_no
left JOIN dwb.dwb_asset_delay_period_info d
  on a.asset_item_no = d.asset_item_no
  and a.period_seq = d.period_seq
WHERE 
  1=1
  and a.grant_time >= '2023-01-01'
  AND a.due_time >= '2025-06-01'
group by 
  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32









--------------------------各阶段未还明细(以原始账单到期日为基准)关联D-1C卡分----------------------------------



create table if not exists dm_aifox.delay_day_detial_jf_3 as
select
  a.*,
  case 
    when a.due_time <> a.delay_due_time and a.delay_seq_cnt = 1 then 'new展期'
    when a.due_time <> a.delay_due_time and a.delay_seq_cnt > 1 then 'ever展期'
    else '未展期' end as delay_label_2,
  d.max_D0_level,    ----D-1C卡分
  if(date(a.due_time) < curdate(), a.granted_principal_period_amt, 0) AS due_cnt,  -- 总到期本金
  if(date(a.due_time) < curdate(), a.interest_period_amt, 0) AS due_interest_cnt,  -- 总到期利息
  if(date(a.due_time) < curdate(), a.fee_period_amt, 0) AS due_fee_cnt,  -- 总到期服务费
  if(date(a.due_time) < curdate(), a.penalty_period_amt, 0) AS due_penalty_cnt,  -- 总到期罚息
  if(date(a.due_time) < curdate(), a.extra_period_amt, 0) AS due_extra_cnt,  -- 总到期额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL 3 DAY) AND (date_diff IS NULL OR date_diff > -3), a.granted_principal_period_amt, 0) AS D_3_overdue, -- D-3未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL 1 DAY) AND (date_diff IS NULL OR date_diff > -1), a.granted_principal_period_amt, 0) AS D_1_overdue, -- D-1未还本金
  if(date(a.due_time) < curdate() AND (date_diff IS NULL OR date_diff > 0), a.granted_principal_period_amt, 0) AS D0_overdue, -- D0未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -1 DAY) AND (date_diff IS NULL OR date_diff > 1), a.granted_principal_period_amt, 0) AS D1_overdue, -- D1未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff > 3), a.granted_principal_period_amt, 0) AS D3_overdue, -- D3未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff > 7), a.granted_principal_period_amt, 0) AS D7_overdue, -- D7未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff > 15), a.granted_principal_period_amt, 0) AS D15_overdue, -- D15未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff > 30), a.granted_principal_period_amt, 0) AS D30_overdue, -- D30未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff > 60), a.granted_principal_period_amt, 0) AS D60_overdue, -- D60未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff > 90), a.granted_principal_period_amt, 0) AS D90_overdue, -- D90未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -120 DAY) AND (date_diff IS NULL OR date_diff > 120), a.granted_principal_period_amt, 0) AS D120_overdue, -- D120未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -150 DAY) AND (date_diff IS NULL OR date_diff > 150), a.granted_principal_period_amt, 0) AS D150_overdue, -- D150未还本金
  if(date(a.due_time) < date_add(curdate(), INTERVAL -180 DAY) AND (date_diff IS NULL OR date_diff > 180), a.granted_principal_period_amt, 0) AS D180_overdue, -- D180未还本金
  if(date(a.due_time) < curdate(), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS due_cnt_all,  -- 总到期利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL 3 DAY) AND (date_diff IS NULL OR date_diff > -3), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D_3_overdue_all, -- D-3未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL 1 DAY) AND (date_diff IS NULL OR date_diff > -1), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D_1_overdue_all, -- D-1未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < curdate() AND (date_diff IS NULL OR date_diff > 0), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D0_overdue_all, -- D0未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -1 DAY) AND (date_diff IS NULL OR date_diff > 1), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D1_overdue_all, -- D1未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff > 3), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D3_overdue_all, -- D3未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff > 7), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D7_overdue_all, -- D7未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff > 15), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D15_overdue_all, -- D15未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff > 30), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D30_overdue_all, -- D30未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff > 60), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D60_overdue_all, -- D60未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff > 90), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D90_overdue_all, -- D90未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -120 DAY) AND (date_diff IS NULL OR date_diff > 120), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D120_overdue_all, -- D120未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -150 DAY) AND (date_diff IS NULL OR date_diff > 150), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D150_overdue_all, -- D150未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < date_add(curdate(), INTERVAL -180 DAY) AND (date_diff IS NULL OR date_diff > 180), a.granted_principal_period_amt+a.interest_period_amt+a.fee_period_amt+a.penalty_period_amt+a.extra_period_amt, 0) AS D180_overdue_all, -- D180未还利息+服务费+罚息+额外收费
  if(date(a.due_time) < curdate(), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS due_cnt_repaid_all,  -- 总已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL 3 DAY) AND (date_diff <= -3), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D_3_overdue_repaid_all, -- D-3已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL 1 DAY) AND (date_diff <= -1), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D_1_overdue_repaid_all, -- D-1已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < curdate() AND (date_diff <= 0), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D0_overdue_repaid_all, -- D0已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -1 DAY) AND (date_diff <= 1), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D1_overdue_repaid_all, -- D1已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff <= 3), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D3_overdue_repaid_all, -- D3已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff <= 7), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D7_overdue_repaid_all, -- D7已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff <= 15), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D15_overdue_repaid_all, -- D15已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff <= 30), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D30_overdue_repaid_all, -- D30已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff <= 60), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D60_overdue_repaid_all, -- D60已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff <= 90), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D90_overdue_repaid_all, -- D90已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -120 DAY) AND (date_diff <= 120), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D120_overdue_repaid_all, -- D120已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -150 DAY) AND (date_diff <= 150), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D150_overdue_repaid_all, -- D150已还利息+服务费+罚息+额外收费+展期费  
  if(date(a.due_time) < date_add(curdate(), INTERVAL -180 DAY) AND (date_diff <= 180), a.repaid_principal_period_amt+a.repaid_interest_period_amt+a.repaid_fee_period_amt+a.repaid_penalty_period_amt+a.repaid_delay_period_amt, 0) AS D180_overdue_repaid_all -- D180已还利息+服务费+罚息+额外收费+展期费  
from dm_aifox.delay_asset_detail_jf_3 as a
left join dm_aifox.d0level as d
on a.asset_item_no = d.asset_item_no
and a.period_seq = d.asset_period
and date_add(a.delay_due_time, INTERVAL 0 DAY)=d.dt






--------------------------核心中间表(以原始账单到期日为基准)----------------------------------






create table if not exists dm_aifox.tha_delay_asset_jf_3 as

select
"金额" as stat_type,
due_month,
delay_due_month,
due_time,
delay_due_time,
user_status,
user_debt_status,
product,
period_seq,
apply_channel_source,
delay_label,
delay_label_2,
amt_type_2,
debtor_new_due,
asset_new_due,
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
SUM(repaid_delay_period_amt)
from dm_aifox.delay_day_detial_jf_3
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18


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
period_seq,
apply_channel_source,
delay_label,
delay_label_2,
amt_type_2,
debtor_new_due,
asset_new_due,
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
null as repaid_delay_period_amt
from dm_aifox.delay_day_detial_jf_3
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18























-------------------------近3个月各阶段应还结构--------------------

select
  a.due_month,
  a.delay_label_2,
  a.amt_type_2,
  a.max_D0_level as C_level,
  sum(due_cnt) AS '总到期本金',
  sum(due_interest_cnt) AS '总到期利息',
  sum(due_fee_cnt) AS  '总到期服务费',
  sum(due_penalty_cnt) AS  '总到期罚息',
  sum(due_extra_cnt) AS '总到期额外收费',
  sum(repaid_delay_period_amt) as '累计展期费',
  sum(if(date(a.due_time) < curdate() AND (date_diff IS NULL OR date_diff > 0),a.granted_principal_period_amt, 0)) as 'D0未还本金',
  sum(if(date(a.due_time) < curdate() AND (date_diff IS NULL OR date_diff > 0),a.interest_period_amt, 0)) as 'D0未还利息',
  sum(if(date(a.due_time) < curdate() AND (date_diff IS NULL OR date_diff > 0),a.fee_period_amt, 0)) as 'D0未还服务费',
  sum(if(date(a.due_time) < curdate() AND (date_diff IS NULL OR date_diff > 0),a.penalty_period_amt, 0)) as 'D0未还罚息',
  sum(if(date(a.due_time) < curdate() AND (date_diff IS NULL OR date_diff > 0),a.extra_period_amt, 0)) as 'D0未还额外收费',
  sum(if(date(a.due_time) < curdate() AND (date_diff <= 0),a.repaid_delay_period_amt, 0)) as 'D0累计展期费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff > 3), a.granted_principal_period_amt, 0)) as 'D3未还本金',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff > 3), a.interest_period_amt, 0)) as 'D3未还利息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff > 3), a.fee_period_amt, 0)) as 'D3未还服务费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff > 3), a.penalty_period_amt, 0)) as 'D3未还罚息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff > 3), a.extra_period_amt, 0)) as 'D3未还额外收费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -3 DAY) AND (date_diff IS NULL OR date_diff <= 3), a.repaid_delay_period_amt, 0)) as 'D3累计展期费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff > 7), a.granted_principal_period_amt, 0)) as 'D7未还本金',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff > 7), a.interest_period_amt, 0)) as 'D7未还利息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff > 7), a.fee_period_amt, 0)) as 'D7未还服务费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff > 7), a.penalty_period_amt, 0)) as 'D7未还罚息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff > 7), a.extra_period_amt, 0)) as 'D7未还额外收费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (date_diff IS NULL OR date_diff <= 7), a.repaid_delay_period_amt, 0)) as 'D7累计展期费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff > 15), a.granted_principal_period_amt, 0)) as 'D15未还本金',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff > 15), a.interest_period_amt, 0)) as 'D15未还利息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff > 15), a.fee_period_amt, 0)) as 'D15未还服务费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff > 15), a.penalty_period_amt, 0)) as 'D15未还罚息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff > 15), a.extra_period_amt, 0)) as 'D15未还额外收费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -15 DAY) AND (date_diff IS NULL OR date_diff <= 15), a.repaid_delay_period_amt, 0)) as 'D15累计展期费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff > 30), a.granted_principal_period_amt, 0)) as 'D30未还本金',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff > 30), a.interest_period_amt, 0)) as 'D30未还利息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff > 30), a.fee_period_amt, 0)) as 'D30未还服务费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff > 30), a.penalty_period_amt, 0)) as 'D30未还罚息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff > 30), a.extra_period_amt, 0)) as 'D30未还额外收费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -30 DAY) AND (date_diff IS NULL OR date_diff <= 30), a.repaid_delay_period_amt, 0)) as 'D30累计展期费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff > 60), a.granted_principal_period_amt, 0)) as 'D60未还本金',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff > 60), a.interest_period_amt, 0)) as 'D60未还利息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff > 60), a.fee_period_amt, 0)) as 'D60未还服务费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff > 60), a.penalty_period_amt, 0)) as 'D60未还罚息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff > 60), a.extra_period_amt, 0)) as 'D60未还额外收费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -60 DAY) AND (date_diff IS NULL OR date_diff <= 60), a.repaid_delay_period_amt, 0)) as 'D60累计展期费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff > 90), a.granted_principal_period_amt, 0)) as 'D90未还本金',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff > 90), a.interest_period_amt, 0)) as 'D90未还利息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff > 90), a.fee_period_amt, 0)) as 'D90未还服务费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff > 90), a.penalty_period_amt, 0)) as 'D90未还罚息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff > 90), a.extra_period_amt, 0)) as 'D90未还额外收费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -90 DAY) AND (date_diff IS NULL OR date_diff <= 90), a.repaid_delay_period_amt, 0)) as 'D90累计展期费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -120 DAY) AND (date_diff IS NULL OR date_diff > 120), a.granted_principal_period_amt, 0)) as 'D120未还本金',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -120 DAY) AND (date_diff IS NULL OR date_diff > 120), a.interest_period_amt, 0)) as 'D120未还利息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -120 DAY) AND (date_diff IS NULL OR date_diff > 120), a.fee_period_amt, 0)) as 'D120未还服务费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -120 DAY) AND (date_diff IS NULL OR date_diff > 120), a.penalty_period_amt, 0)) as 'D120未还罚息',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -120 DAY) AND (date_diff IS NULL OR date_diff > 120), a.extra_period_amt, 0)) as 'D120未还额外收费',
  sum(if(date(a.due_time) < date_add(curdate(), INTERVAL -120 DAY) AND (date_diff IS NULL OR date_diff <= 120), a.repaid_delay_period_amt, 0)) as 'D120累计展期费'
from dm_aifox.delay_day_detial_jf_3 a
where
  due_time >= '2025-06-01' and due_time <='2026-02-28'
  -- and   a.apply_channel_source = 'THA073'
group by 1,2,3,4
order by 1,2,3,4



