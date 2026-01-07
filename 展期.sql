
select month(asset_delay_pay_at) as asset_delay_pay_at
,sum(asset_delay_amount) as asset_delay_amount
from  dwd.dwd_asset_delay
where asset_delay_status = 'success' and date(asset_delay_pay_at) between '2025-01-01' and '2025-12-20'
group by 1 order by 1
;


select sum(asset_delay_amount)
from  dwd.dwd_asset_delay
where asset_delay_status = 'success' 
--and date(asset_delay_pay_at) = '2025-11-20'
and asset_delay_item_no='T2025010272548390454'
;

select * from dwb.dwb_asset_period_info  
where asset_item_no='T2025010272548390454'；
