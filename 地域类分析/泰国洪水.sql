drop table dm_aifox.xshi0_table_a1;
create table dm_aifox.xshi0_table_a1 AS
(
select 
t3.*
,t4.province
,t4.city
from
(select 
t1.*
,t2.debtor_id
from dwb.dwb_asset_period_info      t1 
left join dwd.dwd_fox_debtor_asset  t2
on t1.asset_item_no=t2.asset_item_no
where t1.date(delay_due_time) between '2025-11-01' and  '2025-12-01'
)t3
left join fox_ods.ods_fox_debtor    t4
on t3.debtor_id=t4.id
)
;

select * from dm_aifox.xshi0_table_a1;


select 

select 
sum(case when province is not null and province <>'' then 1 else 0 end)/count(1) as '省份召回率'
,sum(case when city is not null and city <>'' then 1 else 0 end)/count(1) as '城市召回率'
from t5 ;



with t as
(select
delay_due_time,
asset_item_no,
user_id,
case when user_debt_status = "new_user" then "首贷"
else "续贷" end as 用户类型,
province,
grant_time,
finish_time,
IF(overdue_period_days IN (0,1), '是', '否') AS if_overdue,
IF(asset_overdue_period_days IN (0,1), '是', '否') AS if_asset_overdue,
datediff(finish_time,delay_due_time) AS date_diff,
period_seq,
overdue_period_days,
granted_principal_period_amt,
repaid_principal_period_amt,
if(date(delay_due_time) < curdate(), granted_principal_period_amt, 0) AS due_cnt, -- 总到期本金
if(date(delay_due_time) < curdate() AND (datediff(finish_time,delay_due_time) IS NULL OR datediff(finish_time,delay_due_time) > 0), granted_principal_period_amt, 0) AS D0_overdue, -- D0未还
if(date(delay_due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (datediff(finish_time,delay_due_time) IS NULL OR datediff(finish_time,delay_due_time) > 7), granted_principal_period_amt, 0) AS D7_overdue -- D7未还
FROM dm_aifox.xshi0_table_a1
)

select
case when if_overdue = "是" then "新增债务人"
else "在催债务人" end as 债务人类型,
if_asset_overdue,
province,
-- date(
#NAME?
-- date(delay_due_time),
-- INTERVAL - if(DAYOFWEEK(delay_due_time) = 1, 6, DAYOFWEEK(delay_due_time) -2) DAY
-- )
-- ) AS '到期日期',
date(delay_due_time) as 到期日,
sum(due_cnt) AS '到期',
sum(D0_overdue) AS '入催',
sum(D7_overdue) as 'D7逾期金额',
sum(D7_overdue) / sum(due_cnt) 'D7未还',
sum(D0_overdue) / sum(due_cnt) '入催率',
1 - (sum(D7_overdue) / sum(due_cnt)) / (sum(D0_overdue) / sum(due_cnt)) 'D7回收率'
from t
#NAME?
GROUP BY
1,
2,3,4

ORDER BY 2 desc
   
