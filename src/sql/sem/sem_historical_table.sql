delete from {dam-dataset}.historical_sem where ref_dt = '{start_dt}';
INSERT {dam-dataset}.historical_sem
SELECT * from 
(with base as (
    select lylty_card_detail.crn
          , date('{start_dt}') as ref_dt
          ,sum(
             case
                 when article_sales_summary.checkout_nbr = 100 then article_sales_summary.tot_net_excld_gst
                 else 0
             end
             ) as online_spend
         ,sum(
             case
                 when article_sales_summary.checkout_nbr != 100 then article_sales_summary.tot_net_excld_gst
                 else 0
             end
             ) as offline_spend
         ,sum(article_sales_summary.tot_net_excld_gst) as total_spend
    
   
    on article_sales_summary.lylty_card_nbr = lylty_card_detail.lylty_card_nbr
    where article_sales_summary.start_txn_date between date('{start_dt}') and date('{end_dt}')
    and article_sales_summary.lylty_card_nbr not in ('0','-1')
    and lylty_card_detail.crn not in ('0','-1')
    and article_sales_summary.division_nbr in (1005,1030)
    and article_sales_summary.void_flag = 'N'
    group by 2,1
)

    select 
        a.ref_dt
        ,a.crn
        ,a.crn_0
        ,b.total_spend as crn_total_spend
        ,b.online_spend as crn_online_spend
        ,b.offline_spend as crn_offline_spend
        ,c.total_spend as crn0_total_spend
        ,c.online_spend as crn0_online_spend
        ,c.offline_spend as crn0_offline_spend        
        ,case when d.crn is not null then "direct" when e.crn is not null then "indirect" else "generic" end as class
        ,current_timestamp() as last_updated
    from {dam-dataset}.match_sem a
    left join base b
    on a.ref_dt = b.ref_dt
    and a.crn = b.crn
    left join base c
    on a.ref_dt = c.ref_dt
    and a.crn_0 = c.crn
    left join {dam-dataset}.sem_direct d
    on a.ref_dt = d.date
    and a.crn = d.crn
    left join {dam-dataset}.sem_indirect e
    on a.ref_dt = e.date
    and a.crn = e.crn
    left join {dam-dataset}.sem_generic f
    on a.ref_dt = f.date
    and a.crn = f.crn
)
;
