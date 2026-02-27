with period_info as
(select
a.asset_item_no,
a.user_id,
e.user_source_id,
if(a.user_debt_status='new_user','首贷','续贷') AS user_status,
case when a.user_debt_status in ('new_debt','old_debt')  then "共债"
     else "非共债" end as 用户类型,
a.user_debt_status,
g.application_segment,
g.repeat_loan_segment,
c.cur_onloan_cnt,
a.apply_channel_source,  -----------产品包
c.asset_loan_channel,
c.product_form,
c.product_period_cnt as 期数,
c.product_period_unit,
d.product_period as product,
a.grant_time,
datediff(a.finish_time,a.delay_due_time) AS date_diff,
date_format(a.delay_due_time,'%Y-%m') AS delay_due_month,
a.period_seq,
left(a.grant_time,7) as 放款月,
left(a.delay_due_time,7) as 到期月,
date_format(a.delay_due_time,'%Y-%m-%d') AS delay_due_time,
date_format(a.finish_time,'%Y-%m-%d') AS finish_time,
a.overdue_period_days,
IF(a.overdue_period_days IN (0,1), '是', '否') AS if_overdue,
IF(asset_overdue_period_days IN (0,1), '是', '否') AS if_asset_overdue,
a.granted_principal_period_amt,
a.repaid_principal_period_amt,
case when a.granted_principal_period_amt<=500 then '(0,500]'
     when a.granted_principal_period_amt<=1000 then  '(500,1000]'
     when a.granted_principal_period_amt<=1500 then '(1000,1500]'	 
     else '(1500+)' end as amt_type,
if(date(a.delay_due_time) < curdate(), a.granted_principal_period_amt, 0) AS due_amt,  -- 总到期本金
if(date(a.delay_due_time) < curdate() AND (datediff(a.finish_time,a.delay_due_time) IS NULL OR datediff(a.finish_time,a.delay_due_time) > 0), a.granted_principal_period_amt, 0) AS D0_overdue, -- D0未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (datediff(a.finish_time,a.delay_due_time) IS NULL OR datediff(a.finish_time,a.delay_due_time) > 7), a.granted_principal_period_amt, 0) AS D7_overdue -- D7未还

FROM dwb.dwb_asset_period_info AS a

left JOIN dwb.dwb_asset_info AS c
ON a.asset_item_no = c.asset_item_no

left join dim.dim_product_split as d
on a.product_id = d.product_id

left join (select distinct asset_item_number ,user_source_id from  dm_aifox.debtor_wy) as e  ----------中间表
on a.asset_item_no = e.asset_item_number

left join dws.dws_user_segment_label as g 
on a.asset_item_no = g.asset_item_no

WHERE 1=1
and a.delay_due_time >= '2025-11-01'
AND a.delay_due_time < date_add(curdate(), INTERVAL -7 DAY) 
),

  
d_2level as  
(select  
asset_item_no,
asset_period,
due_diff_days,
datediff(dt,asset_due_at) AS cscore_day,
date(dt) cscore_date,
max(cast(level as int)) as cscore_level
from dwd.dwd_fox_c_score_model_result
where due_diff_days = -2
group by 1,2,3,4,5 )
,

d1level as  
(select  
asset_item_no,
asset_period,
due_diff_days,
datediff(dt,asset_due_at) AS cscore_day,
date(dt) cscore_date,
max(cast(level as int)) as cscore_level
from dwd.dwd_fox_c_score_model_result
where due_diff_days = 1
group by 1,2,3,4,5 
),

----------------------------------------债务人信息-----------------------------------

debtor as 
(select 
* 
from 
(select 
ID,
original_customer_id as 客户ID,
enc_idnum,
case when gender = "m" then "男"
     when gender = "f" then "女"
     else gender end as 性别,
province as 户籍省,
city as 户籍城市,
company as 单位名称,
case when job_type = "negocio propio" then "自营事业"
     when job_type = "tiempo completo" then "全职"
     when job_type = "medio tiempo" then "兼职"
     when job_type = "desempleado/a" then "失业者"
     when job_type = "estudiante" then "学生"
     when job_type = "otro" then "其他" 
     else job_type end as 工作类型1,
case when working_years = "menos de 3 meses" then "3个月以下"
     when working_years = "3-6 meses" then "3-6个月"
     when working_years = "6 meses a 1 año" then "6个月至1年"
     when working_years = "1-2 años" then "1-2年"
     when working_years = "más de 2 años" then "2年以上"
     else working_years end as 工作年限1,
STR_TO_DATE(birthday,'%d/%m/%Y')  as 出生日期,

TIMESTAMPDIFF(YEAR, STR_TO_DATE(birthday,'%d/%m/%Y'), CURDATE()) as 年龄,
row_number() over(partition by enc_idnum order by create_at desc ) as rank

from fox_ods.ods_fox_debtor) as t 
where t.rank = 1
),


company as 
(select * 
from 
(select * ,
row_number() over(partition by user_source_id order by 注册时间 desc ) as rank 
from 
(select 
    a.user_individual_user_uuid AS user_uuid,          -- 用户UUID
    c.user_id,
    c.user_source_id,
    c.edu_level as 学历,
    c.reg_first_time as 注册时间,
    c.user_idcard_key,
    b.*
FROM ods.ods_cash_individual_user_individual a



left join 
(SELECT 
    c.company_user_individual_uuid as 与用户资料关联的UUID,
    case when occ.conf_value like "%negocio propio%" then "自营事业"
         when occ.conf_value = "tiempo completo" then "全职"
         when occ.conf_value = "medio tiempo" then "兼职"
         when occ.conf_value = "desempleada/o" then "失业者"
         when occ.conf_value = "estudiante" then "学生"
         when occ.conf_value = "otro" then "其他" 
         else occ.conf_value end as 工作类型,
    case when wy.conf_value = "menos de 3 meses" then "3个月以下"
         when wy.conf_value = "3-6 meses" then "3-6个月"
         when wy.conf_value = "6 meses a 1 año" then "6个月至1年"
         when wy.conf_value = "1-2 años" then "1-2年"
         when wy.conf_value = "más de 2 años" then "2年以上"
         else wy.conf_value end as 工作年限,
    sr.conf_value as 薪资范围,
    case when ins.conf_value = "Propio" then "自己"
         when ins.conf_value = "Pariente" then "亲戚"
         when ins.conf_value = "Pareja" then "伴侣"
         when ins.conf_value = "Padres" then "父母"
         else ins.conf_value end as 收入来源,
    case when sf.conf_value = "Catorcenal" then "每两周一次"
         when sf.conf_value = "Mensual" then "每月一次"
         when sf.conf_value = "Quincenal" then "每月两次"
         when sf.conf_value = "Semanal" then "每周一次"
         else sf.conf_value end as 工资支付频率,
    pd.conf_value as 发薪日,
    fp.conf_value as 第一发薪日,
    sp.conf_value as 第二发薪日,
    sr2.conf_value as 薪资范围2
FROM  ods.ods_cash_individual_company c 

left join dim.dim_rsk_conf_mapping as occ 
on occ.conf_name='occupation' ------职业类型映射
and occ.conf_key= cast(c.company_occupation_id  as VARCHAR)


left join dim.dim_rsk_conf_mapping as wy 
on wy.conf_name='working_years'  -------工作年限映射
and wy.conf_key= cast(c.company_working_years_id  as VARCHAR)


left join dim.dim_rsk_conf_mapping as sr 
on sr.conf_name='salary_range'  -------薪资范围映射
and sr.conf_key= cast(c.company_salary_range_id  as VARCHAR)

left join dim.dim_rsk_conf_mapping as ins 
on ins.conf_name='income_source'  -------收入来源映射
and ins.conf_key= cast(c.company_income_source_id  as VARCHAR)

left join dim.dim_rsk_conf_mapping as sf 
on sf.conf_name='salary_frequency'  -------工资支付频率映射
and sf.conf_key= cast(c.company_salary_frequency_id  as VARCHAR)

left join dim.dim_rsk_conf_mapping as pd 
on pd.conf_name='payday'  -------发薪日映射
and pd.conf_key= cast(c.company_payday_id  as VARCHAR)

left join dim.dim_rsk_conf_mapping as fp 
on fp.conf_name='first_payday'  -------第一发薪日映射
and fp.conf_key= cast(c.company_first_payday_id  as VARCHAR)

left join dim.dim_rsk_conf_mapping as sp 
on sp.conf_name='second_payday'  -------第二发薪日映射
and sp.conf_key= cast(c.company_second_payday_id  as VARCHAR)

left join dim.dim_rsk_conf_mapping as sr2 
on sr2.conf_name='salary_range2'  -------薪资范围2映射
and sr2.conf_key= cast(c.company_salary_range_id2  as VARCHAR)
) as b 
on a.user_individual_uuid = b.与用户资料关联的UUID


left join dwb.dwb_user_info as c 
on a.user_individual_user_uuid = c.user_uuid

) as di 
) as t  where rank = 1
)
,

debtor_info as 
(select a.*,b.* from company as a 
left join debtor as b 
on a.user_idcard_key = b.enc_idnum
), 

----------------------------------汇总中间底表----------------------
T as 
(select 
t1.*,
t2.cscore_level as cscore_D_2,
t3.性别,
t3.户籍省,
t3.户籍城市,
t3.工作类型1,
t3.工作年限1,
t3.年龄,
t3.学历,
t3.工作类型,
t3.工作年限,
t3.薪资范围,
t3.收入来源,
t3.工资支付频率,
t3.发薪日
from period_info as t1 

left join d_2level as t2 
on t1.asset_item_no = t2.asset_item_no
and t1.period_seq = t2.asset_period

left join debtor_info as t3
on t1.user_source_id = t3.user_source_id
),

pivot as 
(select 
user_status as 首续贷,
case when cscore_D_2 between 1 and 3 then "1~3"
     when cscore_D_2 between 4 and 7 then "4~7"
     when cscore_D_2 between 8 and 10 then "8~10"
     else cscore_D_2 end as D_2C卡, 
-- amt_type as 金额段,
性别,
-- 户籍省,
-- 户籍城市,
工作类型1,
工作年限1,
case when 年龄 <20 then "20以下"
     when 年龄 >=20 and 年龄 <30 then "[20,30)"
     when 年龄 >=30 and 年龄 <40 then "[30,40)"
     when 年龄 >=40 and 年龄 <50 then "[40,50)"
     when 年龄 >=50 then  "50以上"
     else 年龄 end as 年龄段,
学历,
-- 工作类型,
-- 工作年限,
-- 薪资范围,
收入来源,
工资支付频率,
sum(due_amt) as 到期金额,
sum(D0_overdue) as D0未还,
sum(D7_overdue) as D7未还
from T 
where if_asset_overdue = "是"
group by 1,2,3,4,5,6,7,8,9

)

select * from pivot ;

with period_info as
(select
a.asset_item_no,
a.user_id,
e.user_source_id,
if(a.user_debt_status='new_user','首贷','续贷') AS user_status,
case when a.user_debt_status in ('new_debt','old_debt')  then "共债"
     else "非共债" end as 用户类型,
a.user_debt_status,
g.application_segment,
g.repeat_loan_segment,
c.cur_onloan_cnt,
a.apply_channel_source,  -----------产品包
c.asset_loan_channel,
c.product_form,
c.product_period_cnt as 期数,
c.product_period_unit,
d.product_period as product,
a.grant_time,
datediff(a.finish_time,a.delay_due_time) AS date_diff,
date_format(a.delay_due_time,'%Y-%m') AS delay_due_month,
a.period_seq,
left(a.grant_time,7) as 放款月,
left(a.delay_due_time,7) as 到期月,
date_format(a.delay_due_time,'%Y-%m-%d') AS delay_due_time,
date_format(a.finish_time,'%Y-%m-%d') AS finish_time,
a.overdue_period_days,
IF(a.overdue_period_days IN (0,1), '是', '否') AS if_overdue,
IF(asset_overdue_period_days IN (0,1), '是', '否') AS if_asset_overdue,
a.granted_principal_period_amt,
a.repaid_principal_period_amt,
case when a.granted_principal_period_amt<=500 then '(0,500]'
     when a.granted_principal_period_amt<=1000 then  '(500,1000]'
     when a.granted_principal_period_amt<=1500 then '(1000,1500]'	 
     else '(1500+)' end as amt_type,
if(date(a.delay_due_time) < curdate(), a.granted_principal_period_amt, 0) AS due_amt,  -- 总到期本金
if(date(a.delay_due_time) < curdate() AND (datediff(a.finish_time,a.delay_due_time) IS NULL OR datediff(a.finish_time,a.delay_due_time) > 0), a.granted_principal_period_amt, 0) AS D0_overdue, -- D0未还
if(date(a.delay_due_time) < date_add(curdate(), INTERVAL -7 DAY) AND (datediff(a.finish_time,a.delay_due_time) IS NULL OR datediff(a.finish_time,a.delay_due_time) > 7), a.granted_principal_period_amt, 0) AS D7_overdue -- D7未还

FROM dwb.dwb_asset_period_info AS a

left JOIN dwb.dwb_asset_info AS c
ON a.asset_item_no = c.asset_item_no

left join dim.dim_product_split as d
on a.product_id = d.product_id

left join (select distinct asset_item_number ,user_source_id from  dm_aifox.debtor_wy) as e  ----------中间表
on a.asset_item_no = e.asset_item_number

left join dws.dws_user_segment_label as g 
on a.asset_item_no = g.asset_item_no

WHERE 1=1
and a.delay_due_time >= '2025-11-01'
AND a.delay_due_time < date_add(curdate(), INTERVAL -7 DAY) 
),

  
d_2level as  
(select  
asset_item_no,
asset_period,
due_diff_days,
datediff(dt,asset_due_at) AS cscore_day,
date(dt) cscore_date,
max(cast(level as int)) as cscore_level
from dwd.dwd_fox_c_score_model_result
where due_diff_days = -2
group by 1,2,3,4,5 )
,

d1level as  
(select  
asset_item_no,
asset_period,
due_diff_days,
datediff(dt,asset_due_at) AS cscore_day,
date(dt) cscore_date,
max(cast(level as int)) as cscore_level
from dwd.dwd_fox_c_score_model_result
where due_diff_days = 1
group by 1,2,3,4,5 
),

----------------------------------------债务人信息-----------------------------------

debtor as 
(select 
* 
from 
(select 
ID,
original_customer_id as 客户ID,
enc_idnum,
case when gender = "m" then "男"
     when gender = "f" then "女"
     else gender end as 性别,
province as 户籍省,
city as 户籍城市,
company as 单位名称,
case when job_type = "negocio propio" then "自营事业"
     when job_type = "tiempo completo" then "全职"
     when job_type = "medio tiempo" then "兼职"
     when job_type = "desempleado/a" then "失业者"
     when job_type = "estudiante" then "学生"
     when job_type = "otro" then "其他" 
     else job_type end as 工作类型1,
case when working_years = "menos de 3 meses" then "3个月以下"
     when working_years = "3-6 meses" then "3-6个月"
     when working_years = "6 meses a 1 año" then "6个月至1年"
     when working_years = "1-2 años" then "1-2年"
     when working_years = "más de 2 años" then "2年以上"
     else working_years end as 工作年限1,
STR_TO_DATE(birthday,'%d/%m/%Y')  as 出生日期,

TIMESTAMPDIFF(YEAR, STR_TO_DATE(birthday,'%d/%m/%Y'), CURDATE()) as 年龄,
row_number() over(partition by enc_idnum order by create_at desc ) as rank

from fox_ods.ods_fox_debtor) as t 
where t.rank = 1
),


company as 
(select * 
from 
(select * ,
row_number() over(partition by user_source_id order by 注册时间 desc ) as rank 
from 
(select 
    a.user_individual_user_uuid AS user_uuid,          -- 用户UUID
    c.user_id,
    c.user_source_id,
    c.edu_level as 学历,
    c.reg_first_time as 注册时间,
    c.user_idcard_key,
    b.*
FROM ods.ods_cash_individual_user_individual a



left join 
(SELECT 
    c.company_user_individual_uuid as 与用户资料关联的UUID,
    case when occ.conf_value like "%negocio propio%" then "自营事业"
         when occ.conf_value = "tiempo completo" then "全职"
         when occ.conf_value = "medio tiempo" then "兼职"
         when occ.conf_value = "desempleada/o" then "失业者"
         when occ.conf_value = "estudiante" then "学生"
         when occ.conf_value = "otro" then "其他" 
         else occ.conf_value end as 工作类型,
    case when wy.conf_value = "menos de 3 meses" then "3个月以下"
         when wy.conf_value = "3-6 meses" then "3-6个月"
         when wy.conf_value = "6 meses a 1 año" then "6个月至1年"
         when wy.conf_value = "1-2 años" then "1-2年"
         when wy.conf_value = "más de 2 años" then "2年以上"
         else wy.conf_value end as 工作年限,
    sr.conf_value as 薪资范围,
    case when ins.conf_value = "Propio" then "自己"
         when ins.conf_value = "Pariente" then "亲戚"
         when ins.conf_value = "Pareja" then "伴侣"
         when ins.conf_value = "Padres" then "父母"
         else ins.conf_value end as 收入来源,
    case when sf.conf_value = "Catorcenal" then "每两周一次"
         when sf.conf_value = "Mensual" then "每月一次"
         when sf.conf_value = "Quincenal" then "每月两次"
         when sf.conf_value = "Semanal" then "每周一次"
         else sf.conf_value end as 工资支付频率,
    pd.conf_value as 发薪日,
    fp.conf_value as 第一发薪日,
    sp.conf_value as 第二发薪日,
    sr2.conf_value as 薪资范围2
FROM  ods.ods_cash_individual_company c 

left join dim.dim_rsk_conf_mapping as occ 
on occ.conf_name='occupation' ------职业类型映射
and occ.conf_key= cast(c.company_occupation_id  as VARCHAR)


left join dim.dim_rsk_conf_mapping as wy 
on wy.conf_name='working_years'  -------工作年限映射
and wy.conf_key= cast(c.company_working_years_id  as VARCHAR)


left join dim.dim_rsk_conf_mapping as sr 
on sr.conf_name='salary_range'  -------薪资范围映射
and sr.conf_key= cast(c.company_salary_range_id  as VARCHAR)

left join dim.dim_rsk_conf_mapping as ins 
on ins.conf_name='income_source'  -------收入来源映射
and ins.conf_key= cast(c.company_income_source_id  as VARCHAR)

left join dim.dim_rsk_conf_mapping as sf 
on sf.conf_name='salary_frequency'  -------工资支付频率映射
and sf.conf_key= cast(c.company_salary_frequency_id  as VARCHAR)

left join dim.dim_rsk_conf_mapping as pd 
on pd.conf_name='payday'  -------发薪日映射
and pd.conf_key= cast(c.company_payday_id  as VARCHAR)

left join dim.dim_rsk_conf_mapping as fp 
on fp.conf_name='first_payday'  -------第一发薪日映射
and fp.conf_key= cast(c.company_first_payday_id  as VARCHAR)

left join dim.dim_rsk_conf_mapping as sp 
on sp.conf_name='second_payday'  -------第二发薪日映射
and sp.conf_key= cast(c.company_second_payday_id  as VARCHAR)

left join dim.dim_rsk_conf_mapping as sr2 
on sr2.conf_name='salary_range2'  -------薪资范围2映射
and sr2.conf_key= cast(c.company_salary_range_id2  as VARCHAR)
) as b 
on a.user_individual_uuid = b.与用户资料关联的UUID


left join dwb.dwb_user_info as c 
on a.user_individual_user_uuid = c.user_uuid

) as di 
) as t  where rank = 1
)
,

debtor_info as 
(select a.*,b.* from company as a 
left join debtor as b 
on a.user_idcard_key = b.enc_idnum
), 

----------------------------------汇总中间底表----------------------
T as 
(select 
t1.*,
t2.cscore_level as cscore_D_2,
t3.性别,
t3.户籍省,
t3.户籍城市,
t3.工作类型1,
t3.工作年限1,
t3.年龄,
t3.学历,
t3.工作类型,
t3.工作年限,
t3.薪资范围,
t3.收入来源,
t3.工资支付频率,
t3.发薪日
from period_info as t1 

left join d_2level as t2 
on t1.asset_item_no = t2.asset_item_no
and t1.period_seq = t2.asset_period

left join debtor_info as t3
on t1.user_source_id = t3.user_source_id
),

pivot as 
(select 
user_status as 首续贷,
case when cscore_D_2 between 1 and 3 then "1~3"
     when cscore_D_2 between 4 and 7 then "4~7"
     when cscore_D_2 between 8 and 10 then "8~10"
     else cscore_D_2 end as D_2C卡, 
-- amt_type as 金额段,
性别,
户籍省,
-- 户籍城市,
-- 工作类型1,
-- 工作年限1,
-- case when 年龄 <20 then "20以下"
--     when 年龄 >=20 and 年龄 <30 then "[20,30)"
--     when 年龄 >=30 and 年龄 <40 then "[30,40)"
--     when 年龄 >=40 and 年龄 <50 then "[40,50)"
--     when 年龄 >=50 then  "50以上"
--     else 年龄 end as 年龄段,
-- 学历,
-- 工作类型,
-- 工作年限,
薪资范围,
收入来源,
工资支付频率,
sum(due_amt) as 到期金额,
sum(D0_overdue) as D0未还,
sum(D7_overdue) as D7未还
from T 
where if_asset_overdue = "是"
group by 1,2,3,4,5,6,7

)

select * from pivot ;







