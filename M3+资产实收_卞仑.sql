核销资产回款，账龄回收收入
with bad_repaid as (
select
repayment.biz_order_no, --资产编号
repayment.period, --期次
repayment.amount_type, --本息费罚等资金类型
repayment.biz_order_channel, --放款渠道
repayment.trans_type, --账务一级分类
repayment.trans_sub_type, --账务二级分类
repayment.trans_finish_at, --支付完成时间
repayment.amount, --支付金额

bad_debt.accrual_at, --核销时间

asset_period.grant_time, --放款时间
asset_period.due_time, --到期时间
asset_period.period_start_time --每期开始时间
from
(select
biz_order_no,
period,
amount_type,

biz_order_channel,
trans_type,
trans_sub_type,
trans_finish_at,
amount
from biz_acctrix.biz_acctrix_accounting_trans_acru_c --账务acru_c表
where trans_type='repayment' --限制只看实还
and trans_sub_type='cust_repayment_cash' --限制只看现金部分
-- and substr(biz_order_no,2,6)>='202212' --加速
) repayment

left outer join
(select
item_no,
item_period,
case
when tran_type='repayprincipal' then 'principal'
when tran_type='repayinterest' then 'interest'
else tran_type
end tran_type,
accrual_at,
accrual_amount
from dwd.dwd_asset_bad_debt_asset --坏账核销表
-- where substr(item_no,2,6)>='202212' --加速
) bad_debt
on repayment.biz_order_no=bad_debt.item_no
and repayment.period=bad_debt.item_period
and repayment.amount_type=bad_debt.tran_type

inner join
(select
asset_item_no,
period_seq,
grant_time,
due_time,
period_start_time
from dwb.dwb_asset_period_info
-- where date_format(grant_time,"%Y")>='2023' --加速
) asset_period
on repayment.biz_order_no=asset_period.asset_item_no
and repayment.period=asset_period.period_seq
)

select
date_format(trans_finish_at,'%Y%m') finish_time,
sum(amount) bad_repaid_amount,
sum(case when date_diff('month', trans_finish_at, accrual_at)<=24 then amount else 0 end) bad_repaid_amount_24,
sum(case when date_diff('month', trans_finish_at, accrual_at)<=12 then amount else 0 end) bad_repaid_amount_12
from bad_repaid
where trans_finish_at>=accrual_at --限制支付时间在核销后
group by 1;
