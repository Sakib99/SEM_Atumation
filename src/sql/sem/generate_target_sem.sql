with base as (
    -- group together the dates to the start of the week
select date as ref_dt
         , direct.crn
    from {dam-project}.{dam-dataset}.sem_direct  direct
    where  direct.date = date('{start_dt}')
    
    union distinct 
    select distinct date as ref_dt
         , indirect.crn
    from {dam-project}.{dam-dataset}.sem_indirect  indirect
    where  indirect.date = date('{start_dt}')
    
    union distinct
    select distinct date as ref_dt
         , generic.crn
    from {dam-project}.{dam-dataset}.sem_generic  generic
    where  generic.date = date('{start_dt}')
)
,
cvm as (
    select customer_value_model.crn
         ,Date_trunc(customer_value_model.pw_end_date, isoweek) as ref_dt
         ,min(customer_value_model.macro_segment_curr) as cvm
    from `gcp-wow-rwds-ai-safari-prod.wdp_tables.customer_value_model` customer_value_model
    group by 1,2
    order by 1,2
)
,
spend_8wk as (
    -- changed query to include a single WHERE clause on the dates
    select lylty_card_detail.crn,
            base.ref_dt as ref_dt, -- removed case clause
            sum(article_sales_summary.tot_net_excld_gst)/8 as spend_8wk
        from `gcp-wow-rwds-ai-safari-prod.wdp_tables.article_sales_summary` article_sales_summary
        left join `gcp-wow-rwds-ai-safari-prod.wdp_tables.lylty_card_detail` lylty_card_detail
        on article_sales_summary.lylty_card_nbr = lylty_card_detail.lylty_card_nbr
        inner join base
        on  base.crn = lylty_card_detail.crn
        where article_sales_summary.lylty_card_nbr != '0'
        -- changed the time interval to exclude the start_dt, moved start of windows back one day ( 57 instead of 56)
        and article_sales_summary.start_txn_date between DATE_SUB('{start_dt}', INTERVAL 57 DAY) and DATE_SUB('{start_dt}', INTERVAL 1 DAY)
        and lylty_card_detail.crn != '0'
        and article_sales_summary.division_nbr in (1005,1030)
        group by 1,2
)
,
spend_26wk as (
    select lylty_card_detail.crn,
           base.ref_dt as ref_dt, -- removed case clause
           sum(article_sales_summary.tot_net_excld_gst)/26 as spend_26wk
    from `gcp-wow-rwds-ai-safari-prod.wdp_tables.article_sales_summary` article_sales_summary
    left join `gcp-wow-rwds-ai-safari-prod.wdp_tables.lylty_card_detail` lylty_card_detail
    on article_sales_summary.lylty_card_nbr = lylty_card_detail.lylty_card_nbr
    inner join base
    on base.crn = lylty_card_detail.crn
    where article_sales_summary.lylty_card_nbr != '0'
    and lylty_card_detail.crn != '0'
    and article_sales_summary.division_nbr in (1005,1030)
    -- changed the time interval to exclude the start_dt, moved start of windows back one day ( 183 instead of 182)
    and article_sales_summary.start_txn_date between DATE_SUB('{start_dt}', INTERVAL 183 DAY) and DATE_SUB('{start_dt}', INTERVAL 1 DAY)    group by 1,2
)
select base.ref_dt,
       base.crn,
       cvm.cvm,
       coalesce(spend_8wk.spend_8wk,0) spend_8wk,
       coalesce(spend_26wk.spend_26wk,0) spend_26wk
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