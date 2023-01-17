
create and replace table `{dam-project}.{dam-dataset}.test`  as
(
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
    from gcp-wow-rwds-ai-safari-prod.wdp_tables.article_sales_summary
    left join gcp-wow-rwds-ai-safari-prod.wdp_tables.lylty_card_detail
    on article_sales_summary.lylty_card_nbr = lylty_card_detail.lylty_card_nbr
    where article_sales_summary.start_txn_date between date('{start_dt}') and date('{end_dt}')
    and article_sales_summary.lylty_card_nbr not in ('0','-1')
    and lylty_card_detail.crn not in ('0','-1')
    and article_sales_summary.division_nbr in (1005,1030)
    and article_sales_summary.void_flag = 'N'
    group by 2,1
    );