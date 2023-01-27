with base0 as (
    select Date('{start_dt}') as ref_dt
         ,target.crn
    from {dam-project}.{dam-dataset}.generate_target_sem target
    
    union distinct 
    
    select  Date('{start_dt}') as ref_dt, crn
        from ``
        where date_trunc(fw_start_date, isoweek) between date('{start_dt}') and date('{end_dt}')
        group by 1,2
)
,
base as (
    select lylty_card_detail.crn
          ,date_trunc(article_sales_summary.start_txn_date, isoweek) as ref_dt
         ,sum(
             case
                 when article_sales_summary.checkout_nbr = 100 then 1
                 else 0
             end
         ) as online_spend_count -- Why is this being calculated? It's not getting used anywhere
    from `` article_sales_summary
    left join `` lylty_card_detail 
    on article_sales_summary.lylty_card_nbr = lylty_card_detail.lylty_card_nbr
    inner join `p` control_group
    on lylty_card_detail.crn = control_group.crn
    and article_sales_summary.start_txn_date between control_group.control_start_date and control_group.control_end_date
    left join base0
    on lylty_card_detail.crn = base0.crn
    and date_trunc(article_sales_summary.start_txn_date, isoweek) = base0.ref_dt
    where date(article_sales_summary.start_txn_date) between date('{start_dt}') and date('{end_dt}')
    and article_sales_summary.lylty_card_nbr != '0'
    and lylty_card_detail.crn != '0'
    and article_sales_summary.division_nbr in (1005,1030)
    and article_sales_summary.void_flag = 'N'
    and control_group.control_type = 1
    and (base0.crn is null and base0.ref_dt is null)
    group by 2,1
    order by 2,1
)
,
cvm as (
    select customer_value_model.crn
         ,Date_trunc(customer_value_model.pw_end_date, isoweek) as ref_dt
         ,min(customer_value_model.macro_segment_curr) as cvm
    from `gcp-wow-rwds-ai-safari-prod.wdp_tables.customer_value_model` customer_value_model
    where customer_value_model.pw_end_date between date('{start_dt}') and date('{end_dt}')
    group by 1,2
    order by 1,2
)
,
spend_8wk as (
    select lylty_card_detail.crn
         , base.ref_dt as ref_dt -- removed case clause
         ,sum(article_sales_summary.tot_net_excld_gst)/8 as spend_8wk
    from `` article_sales_summary
    left join `` lylty_card_detail
    on article_sales_summary.lylty_card_nbr = lylty_card_detail.lylty_card_nbr
    inner join base
    on base.crn = lylty_card_detail.crn
    where article_sales_summary.lylty_card_nbr != '0'
    and lylty_card_detail.crn != '0'
    and article_sales_summary.division_nbr in (1005,1030)
    and article_sales_summary.void_flag = 'N'
    -- changed the time interval to exclude the start_dt, moved start of windows back one day ( 57 instead of 56)
    and article_sales_summary.start_txn_date between DATE_SUB('{start_dt}', INTERVAL 57 DAY) and DATE_SUB('{start_dt}', INTERVAL 1 DAY)    
    group by 1,2
)
,
spend_26wk as (
    select lylty_card_detail.crn
        ,base.ref_dt as ref_dt -- removed case clause
        ,sum(article_sales_summary.tot_net_excld_gst)/26 as spend_26wk
    from `` article_sales_summary
    left join `` lylty_card_detail
    on article_sales_summary.lylty_card_nbr = lylty_card_detail.lylty_card_nbr
    inner join base
    on article_sales_summary.start_txn_date between DATE_SUB(base.ref_dt, INTERVAL 182 DAY) and DATE(base.ref_dt)
    and base.crn = lylty_card_detail.crn
    where article_sales_summary.lylty_card_nbr != '0'
    and lylty_card_detail.crn != '0'
    and article_sales_summary.division_nbr in (1005,1030)
    and article_sales_summary.void_flag = 'N'
    -- changed the time interval to exclude the start_dt, moved start of windows back one day ( 183 instead of 182)
    and article_sales_summary.start_txn_date between DATE_SUB('{start_dt}', INTERVAL 183 DAY) 
                                            and DATE_SUB('{start_dt}', INTERVAL 1 DAY)    
    group by 1,2
)
select base.ref_dt
     ,base.crn
     ,cvm.cvm
     ,coalesce(spend_8wk.spend_8wk,0) spend_8wk
     ,coalesce(spend_26wk.spend_26wk,0) spend_26wk
from base
left join cvm
on base.ref_dt = cvm.ref_dt
and base.crn = cvm.crn
left join spend_8wk
on base.ref_dt = spend_8wk.ref_dt
and base.crn = spend_8wk.crn
left join spend_26wk
on base.ref_dt = spend_26wk.ref_dt
and base.crn = spend_26wk.crn
where base.ref_dt is not null
and base.crn is not null
and cvm.cvm is not null
order by 1,2
;
