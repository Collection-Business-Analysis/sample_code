-- BI code

INSERT
OVERWRITE ads.ads_3525_deduct_summary_d (
deduct_applied_date,
deduct_source,
deduct_type,
sub_deduct_type,
deduct_amt
)
SELECT
date(deduct_applied_time) AS deduct_applied_date,
CASE
WHEN deduct_source = 'fox' THEN '贷后系统'
WHEN deduct_source = 'rbiz' THEN 'BIZ系统'
WHEN deduct_source = 'crm' THEN 'CRM系统'
WHEN deduct_source = 'marketing' THEN '营销'
WHEN deduct_source = 'bc' THEN 'BC系统'
WHEN deduct_source = 'biz' THEN 'BIZ系统'
WHEN deduct_source = 'strategy' THEN '策略'
WHEN deduct_source = '' THEN '历史数据'
WHEN deduct_source IS NULL THEN '历史数据'
ELSE '未知'
END AS deduct_source,
CASE
WHEN deduct_type = 'tolerance_payoff' THEN '容差减免' -- 老版本，已无增量
WHEN deduct_type = 'direct' THEN '直接减免' -- 老版本，已无增量
WHEN deduct_type = 'advance_settle' THEN '提前还款' -- 老版本，已无增量
WHEN deduct_type = 'decrease' THEN '减免' -- 新版本，当前主力字段
WHEN deduct_type = 'period_deduction' THEN '期次减免' -- 老版本，已无增量
WHEN deduct_type = 'coupon' THEN '优惠券' -- 老版本，已无增量
WHEN deduct_type = 'asset_void' THEN '资产作废' -- 老版本，已无增量
WHEN deduct_type = 'arbitration' THEN '房贷减免' -- 老版本，已无增量
WHEN deduct_type = 'refund' THEN '退款'
WHEN deduct_type = 'provision' THEN '减免'
WHEN deduct_type = 'advance_repay' THEN '提前还款'
ELSE '未知' -- 兜底：未来新增类型
END AS deduct_type,
CASE
WHEN sub_deduct_type = 'M' THEN '人工减免' -- 上游业务人员调用 API 进行的减免（主力字段）
WHEN sub_deduct_type = 'T' THEN '容差/拨备' -- 容差、拨备
WHEN sub_deduct_type = 'W' THEN '用户取消放款费用减免' -- 用户取消放款，只还本金，费用公司承担
WHEN sub_deduct_type = 'C' THEN '优惠卷' -- 用户取消放款，只还本金，费用公司承担
WHEN sub_deduct_type = 'coupon_null' THEN '未知优惠券类型' -- 老版本，无增量
WHEN sub_deduct_type = 'direct_null' THEN '直接减免' -- 老版本，无增量
WHEN sub_deduct_type = 'fin_service' THEN '服务费减免' -- 老版本，无增量
WHEN sub_deduct_type = 'fpr' THEN '贷后期次减免券' -- 新/现版本有使用
WHEN sub_deduct_type = 'repay' THEN '还款抵扣金' -- 抵扣用
WHEN sub_deduct_type = 'repayinterest' THEN '利息减免' -- 老版本，无增量
WHEN sub_deduct_type = 'repayprincipal' THEN '本金减免' -- 老版本，无增量
WHEN sub_deduct_type = 'lateinterest' THEN '罚息减免' -- 老版本，无增量
WHEN sub_deduct_type = 'rid' THEN '利息折扣券' -- Interest Discount
WHEN sub_deduct_type = 'settle' THEN '结清券' -- 结清相关券
WHEN sub_deduct_type = 'period' THEN '期次券' -- 老版本，无增量（与资产期次绑定）
WHEN sub_deduct_type = 'cash' THEN '现金券' -- 老版本，无增量
WHEN sub_deduct_type = 'asset_decrease' THEN '资产减免'
WHEN sub_deduct_type = 'arbitration' THEN '房贷减免'
WHEN sub_deduct_type = 'manual_decrease' THEN '手动减免'
WHEN sub_deduct_type = 'refund_after_settled' THEN '结清后退款'
WHEN sub_deduct_type = 'capital_decrease' THEN '资方要求减免'
WHEN sub_deduct_type = 'VOUCHER-TICKET' THEN '现金券'
WHEN sub_deduct_type = 'refund_after_settled_complaint' THEN '结清后退款（有投诉）'
WHEN sub_deduct_type = 'asset_void' THEN '资产作废'
ELSE '未知'
END AS sub_deduct_type,
sum(deduct_amt) AS deduct_amt
FROM
dwb.dwb_asset_deduct_period_info
GROUP BY
1,
2,
3,
4;

SELECT
-- date_format(deduct_applied_date, '%Y-%m-%d') AS '统计日期',
case
when {{date_type}}='日' then date_format(deduct_applied_date, '%Y-%m-%d')
when {{date_type}}='周' then date(date_add(date_format(deduct_applied_date, '%Y-%m-%d'), INTERVAL -dayofweek_iso(date_format(deduct_applied_date, '%Y-%m-%d'))+1 DAY))
when {{date_type}}='月' then (date_format(deduct_applied_date,'%Y-%m'))
end '统计日期',
deduct_source as '团队',
sum(deduct_amt) AS '减免金额'
FROM ads_3525_deduct_summary_d
WHERE 1 = 1
[[and {{stat_date}}]]
[[and {{deduct_type}}]]
[[and {{sub_deduct_type}}]]
[[and {{deduct_source}}]]
GROUP BY 1,2
