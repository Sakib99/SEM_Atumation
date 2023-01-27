
with match_rate as (
    select ref_dt, match_rate
        from `'
        where advertiser = 'Supermarket_Search'
        and ref_dt = date('{start_dt}')
),
base as (
    select * from `{dam-project}.{dam-dataset}.historical_sem` where ref_dt = date('{start_dt}')
    and crn_total_spend > 0
)
,
btl_inc_sales as (
    select  crn,
            date_trunc(fw_start_date, isoweek) as ref_dt,
            sum(inc_sales) as inc_sales
        from `t`
        where date_trunc(fw_start_date, isoweek) = date('{start_dt}')
        group by 1,2
)
,
inc_sale as (
    select 
        a.ref_dt
        ,a.crn
        ,a.class
        ,crn_total_spend - crn0_total_spend as inc_sale
        ,(crn_total_spend - crn0_total_spend)*crn_online_spend/(crn_total_spend) as inc_sale_online
        ,(crn_total_spend - crn0_total_spend)*crn_offline_spend/(crn_total_spend) as inc_sale_offline
        ,crn_total_spend as tot_sales
        ,crn_offline_spend as off_sales
        ,crn_online_spend as onl_sales
        ,e.inc_sales as btl_inc_sales


    from base a
    left join btl_inc_sales e
    on a.ref_dt = e.ref_dt
    and a.crn = e.crn
   
)
,
sem as (
    select a.ref_dt
        ,sum(1) as sem_crns
        ,sum(a.tot_sales) as sem_tot_sale
        ,sum(a.off_sales) as sem_off_sales
        ,sum(a.onl_sales) as sem_onl_sales
        ,sum(a.inc_sale) as sem_inc_sale
        ,sum(a.inc_sale_online) as sem_inc_sale_online 
        ,sum(a.inc_sale_offline) as sem_inc_sale_offline 
        ,sum(a.btl_inc_sales) as sem_btl_inc_sale
        ,sum(a.inc_sale) - sum(a.btl_inc_sales) as sem_net_inc_sale
    
        ,sum(case when class = 'direct' then 1 else 0 end) as sem_direct_crns
        ,sum(case when class = 'direct' then a.tot_sales else 0 end) as sem_direct_tot_sale
        ,sum(case when class = 'direct' then a.off_sales else 0 end) as sem_direct_off_sales
        ,sum(case when class = 'direct' then a.onl_sales else 0 end) as sem_direct_onl_sales
        ,sum(case when class = 'direct' then a.inc_sale else 0 end) as sem_direct_inc_sale
        ,sum(case when class = 'direct' then a.inc_sale_online else 0 end) as sem_direct_inc_sale_online 
        ,sum(case when class = 'direct' then a.inc_sale_offline else 0 end) as sem_direct_inc_sale_offline 
        ,sum(case when class = 'direct' then a.btl_inc_sales else 0 end) as sem_direct_btl_inc_sale
        ,sum(case when class = 'direct' then a.inc_sale else 0 end) - sum(case when class = 'direct' then a.btl_inc_sales else 0 end) as sem_direct_net_inc_sale

        ,sum(case when class = 'indirect' then 1 else 0 end) as sem_indirect_crns
        ,sum(case when class = 'indirect' then a.tot_sales else 0 end) as sem_indirect_tot_sale
        ,sum(case when class = 'indirect' then a.off_sales else 0 end) as sem_indirect_off_sales
        ,sum(case when class = 'indirect' then a.onl_sales else 0 end) as sem_indirect_onl_sales
        ,sum(case when class = 'indirect' then a.inc_sale else 0 end) as sem_indirect_inc_sale
        ,sum(case when class = 'indirect' then a.inc_sale_online else 0 end) as sem_indirect_inc_sale_online 
        ,sum(case when class = 'indirect' then a.inc_sale_offline else 0 end) as sem_indirect_inc_sale_offline 
        ,sum(case when class = 'indirect' then a.btl_inc_sales else 0 end) as sem_indirect_btl_inc_sale
        ,sum(case when class = 'indirect' then a.inc_sale else 0 end) - sum(case when class = 'indirect' then a.btl_inc_sales else 0 end) as sem_indirect_net_inc_sale
    
        ,sum(case when class = 'generic' then 1 else 0 end) as sem_generic_crns
        ,sum(case when class = 'generic' then a.tot_sales else 0 end) as sem_generic_tot_sale
        ,sum(case when class = 'generic' then a.off_sales else 0 end) as sem_generic_off_sales
        ,sum(case when class = 'generic' then a.onl_sales else 0 end) as sem_generic_onl_sales
        ,sum(case when class = 'generic' then a.inc_sale else 0 end) as sem_generic_inc_sale
        ,sum(case when class = 'generic' then a.inc_sale_online else 0 end) as sem_generic_inc_sale_online 
        ,sum(case when class = 'generic' then a.inc_sale_offline else 0 end) as sem_generic_inc_sale_offline 
        ,sum(case when class = 'generic' then a.btl_inc_sales else 0 end) as sem_generic_btl_inc_sale
        ,sum(case when class = 'generic' then a.inc_sale else 0 end) - sum(case when class = 'generic' then a.btl_inc_sales else 0 end) as sem_generic_net_inc_sale
    
    from inc_sale a
    group by 1
    order by 1
)
select a.*
        ,b.match_rate
from sem a
join match_rate b
on a.ref_dt = b.ref_dt


;
