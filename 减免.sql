-- BI code

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
