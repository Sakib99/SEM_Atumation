drop table if exists `{dam-project}.{dam-dataset}.sem_productGroup`;
create table `{dam-project}.{dam-dataset}.sem_productGroup` as
(
--     SELECT * from `wx-bq-poc.digital_attribution_modelling.sy_sem_productGroup`
SELECT productGroupId, productGroup
        , REPLACE(concat(productCategoryLevel1,productCategoryLevel2,productCategoryLevel3,productCategoryLevel4),'Everything else','') as pc
        , REPLACE(concat(productTypeLevel1,productTypeLevel2,productTypeLevel3), 'Everything else','') as pt
        , REPLACE(productId, 'Everything else','') as productId
    from
    (
        select distinct SUBSTR(productGroupId,7,17) as productGroupId, productGroup
                , CASE WHEN REGEXP_CONTAINS(productGroup, 'CATEGORY_LEVEL_1') then SPLIT(SPLIT(productGroup,'CATEGORY_LEVEL_1 = \"')[OFFSET(1)], '\"')[OFFSET(0)] else '' end as productCategoryLevel1
                , CASE WHEN REGEXP_CONTAINS(productGroup, 'CATEGORY_LEVEL_2') then SPLIT(SPLIT(productGroup,'CATEGORY_LEVEL_2 = \"')[OFFSET(1)], '\"')[OFFSET(0)] else '' end as productCategoryLevel2
                , CASE WHEN REGEXP_CONTAINS(productGroup, 'CATEGORY_LEVEL_3') then SPLIT(SPLIT(productGroup,'CATEGORY_LEVEL_3 = \"')[OFFSET(1)], '\"')[OFFSET(0)] else '' end as productCategoryLevel3
                , CASE WHEN REGEXP_CONTAINS(productGroup, 'CATEGORY_LEVEL_4') then SPLIT(SPLIT(productGroup,'CATEGORY_LEVEL_4 = \"')[OFFSET(1)], '\"')[OFFSET(0)] else '' end as productCategoryLevel4
                , CASE WHEN REGEXP_CONTAINS(productGroup, 'PRODUCT_TYPE_LEVEL_1') then SPLIT(SPLIT(productGroup,'PRODUCT_TYPE_LEVEL_1 = \"')[OFFSET(1)], '\"')[OFFSET(0)] else '' end as productTypeLevel1
                , CASE WHEN REGEXP_CONTAINS(productGroup, 'PRODUCT_TYPE_LEVEL_2') then SPLIT(SPLIT(productGroup,'PRODUCT_TYPE_LEVEL_2 = \"')[OFFSET(1)], '\"')[OFFSET(0)] else '' end as productTypeLevel2
                , CASE WHEN REGEXP_CONTAINS(productGroup, 'PRODUCT_TYPE_LEVEL_3') then SPLIT(SPLIT(productGroup,'PRODUCT_TYPE_LEVEL_3 = \"')[OFFSET(1)], '\"')[OFFSET(0)] else '' end as productTypeLevel3
                , CASE WHEN REGEXP_CONTAINS(productGroup, 'PRODUCT_ID') then SPLIT(SPLIT(productGroup,'PRODUCT_ID = \"')[OFFSET(1)], '\"')[OFFSET(0)] else '' end as productId
        from `wr-weeklysaver-poc-2018.sa360_transfer.p_ProductGroup_21700000001518015`
     
    )
);


drop table if exists `{dam-project}.{dam-dataset}.sem_productAdvertised`;
create table `{dam-project}.{dam-dataset}.sem_productAdvertised` as
(
--     SELECT * from `wx-bq-poc.digital_attribution_modelling.sy_sem_productAdvertised`
select (REPLACE(concat(productCategoryLevel1,productCategoryLevel2,productCategoryLevel3,productCategoryLevel4),'Everything else','')) as pc
        , (REPLACE(concat(productTypeLevel1,productTypeLevel2,productTypeLevel3), 'Everything else','')) as pt
        , productId
        from 
    (
      SELECT distinct 
      CASE WHEN productCategoryLevel1 is not null then productCategoryLevel1 else '' end as productCategoryLevel1
                , CASE WHEN productCategoryLevel2 is not null then productCategoryLevel2 else '' end as productCategoryLevel2
        , CASE WHEN productCategoryLevel3 is not null then productCategoryLevel3 else '' end as productCategoryLevel3
        , CASE WHEN productCategoryLevel4 is not null then productCategoryLevel4 else '' end as productCategoryLevel4
        , CASE WHEN productTypeLevel1 is not null then productTypeLevel1 else '' end as productTypeLevel1
        , CASE WHEN productTypeLevel2 is not null then productTypeLevel2 else '' end as productTypeLevel2
        , CASE WHEN productTypeLevel3 is not null then productTypeLevel3 else '' end as productTypeLevel3
                , CASE WHEN productId is not null then productId else '' end as productId
      from `wr-weeklysaver-poc-2018.sa360_transfer.p_ProductAdvertised_21700000001518015`
    )
);



drop table if exists `{dam-project}.{dam-dataset}.sem_productGroup_all`;

create table `{dam-project}.{dam-dataset}.sem_productGroup_all` as
select distinct productGroupId, productGroup, productId
from `{dam-project}.{dam-dataset}.sem_productGroup` where productId <> ''

union distinct

select distinct a.productGroupId, a.productGroup, b.productId
from
(
  SELECT * FROM `{dam-project}.{dam-dataset}.sem_productGroup` where pc <> '' and productId = ''
) a 
cross join
(
  SELECT * FROM `{dam-project}.{dam-dataset}.sem_productAdvertised` where pc <> ''
) b 
where REGEXP_CONTAINS(b.pc, a.pc)

union distinct

select distinct a.productGroupId, a.productGroup, b.productId
from
(
  SELECT * FROM `{dam-project}.{dam-dataset}.sem_productGroup` where pt <> '' and productId = ''
) a 
cross join
(
  SELECT * FROM `{dam-project}.{dam-dataset}.sem_productAdvertised` where pt <> ''
) b 
where REGEXP_CONTAINS(b.pt, a.pt);






drop table if exists `{dam-project}.{dam-dataset}.sem_prod_clk`;
create table `{dam-project}.{dam-dataset}.sem_prod_clk` as
(
    SELECT b.crn, a.user_id, event_time as time_utc, date(event_time) as date, productGroupId, productGroup, c.productId as prod_nbr, am.article_name
    from
    (
        SELECT user_id, event_time, segment_value_1 FROM `gcp-wow-rwds-ai-mlt-evs-prod.googlex.click`
        where user_id <> '0' and segment_value_1 is not null 
            and Date(event_time) between DATE_SUB(Date('{start_dt}'), INTERVAL 8 DAY) and Date('{end_dt}')
            -- Clicks that happen before the start of the SEM windows should be considered, e.g. click on Friday
            -- that can be linked to purchase the next Monday
    ) a

    inner join 
    (
        select distinct * 
        from `gcp-wow-rwds-ai-mlt-evs-prod.event_store.googlex_userid_crn_map_window`
    ) b
    on a.user_id = b.user_id
        and a.event_time between b.effective_time and b.expiry_time

    left join
    (
        select distinct productGroupId, productGroup, productId
        from `{dam-project}.{dam-dataset}.sem_productGroup_all`
    ) c on a.segment_value_1 = c.productGroupId
    
  
    left join
    (
        select distinct article_name,  -- 1st change
                        article_nbr as prod_nbr
        from   gcp-wow-rwds-ai-safari-prod.wdp_tables.article_master 
        where   division_nbr in (1005,1030)
    ) am on c.productId=am.prod_nbr
);



drop table if exists `{dam-project}.{dam-dataset}.sem_prod_spend`;
create table `{dam-project}.{dam-dataset}.sem_prod_spend` as
(
    select a.crn, ass.start_txn_time, SPLIT(ass.prod_nbr,'-')[OFFSET(0)] as prod_nbr, ass.checkout_type, sum(tot_amt_incld_gst-ass.tot_wow_dollar_incld_gst) as spend
    from
    (
        select distinct crn
        from `{dam-project}.{dam-dataset}.sem_prod_clk`
        where productGroupId is not null
    ) a 
    
    inner join 
    (
        select lylty_card_nbr,crn -- change 3rd
        from `gcp-wow-rwds-ai-safari-prod.wdp_tables.lylty_card_detail` 
        where lylty_card_status=1 and crn is not NULL and crn <> '0'
    ) as lcd on a.crn = lcd.crn
    
    inner join 
    (
        select lylty_card_nbr, start_txn_time, prod_nbr, tot_amt_incld_gst, tot_wow_dollar_incld_gst, division_nbr
                , case when checkout_nbr = 100 then 'online' else 'instore' end as checkout_type
        from `gcp-wow-rwds-ai-safari-prod.wdp_tables.article_sales_summary` 
        where division_nbr in (1005,1030) and void_flag <> 'Y'
        -- Changed time interval definition, to remove days before start_dt
        and Date(start_txn_time) >=  Date('{start_dt}')
        -- And restrict the interval to end_dt (previously it was open on the right)
        and Date(start_txn_time) <=  Date('{end_dt}')
    ) ass on ass.lylty_card_nbr=lcd.lylty_card_nbr
    
    inner join
    (
        select prod_nbr, division_nbr -- 2nd change
        from   gcp-wow-rwds-ai-safari-prod.wdp_tables.article_master 
        where   division_nbr in (1005,1030)
    ) am on ass.prod_nbr=am.prod_nbr and ass.division_nbr=am.division_nbr
    
    group by 1,2,3,4
);





drop table if exists `{dam-project}.{dam-dataset}.sem_direct`;
create table `{dam-project}.{dam-dataset}.sem_direct` as
(

    
    SELECT distinct date('{start_dt}') as date, a.crn 
    from 
    (
        SELECT * from `{dam-project}.{dam-dataset}.sem_prod_clk` where productGroupId is not null
    ) a

    inner join
    (
        SELECT crn, start_txn_time, prod_nbr
        from `{dam-project}.{dam-dataset}.sem_prod_spend`
        group by 1,2,3
    ) e on a.crn = e.crn 
            and a.prod_nbr = e.prod_nbr
            and timestamp_diff(e.start_txn_time, a.time_utc, SECOND) > 0
            and timestamp_diff(e.start_txn_time, a.time_utc, DAY) between 0 and 7
    
);

drop table if exists `{dam-project}.{dam-dataset}.sem_indirect`;
create table `{dam-project}.{dam-dataset}.sem_indirect` as
(

    
    SELECT distinct date('{start_dt}') as date, a.crn 
    from 
    (
        SELECT * from `{dam-project}.{dam-dataset}.sem_prod_clk` where productGroupId is not null
    ) a

    inner join
    (
        SELECT crn, start_txn_time, prod_nbr
        from `{dam-project}.{dam-dataset}.sem_prod_spend`
        group by 1,2,3
    ) e on a.crn = e.crn 
            and timestamp_diff(e.start_txn_time, a.time_utc, SECOND) > 0
            and timestamp_diff(e.start_txn_time, a.time_utc, DAY) between 0 and 7
    left join `{dam-project}.{dam-dataset}.sem_direct` direct
    on a.crn = direct.crn
    where direct.crn is null
    
);


drop table if exists `{dam-project}.{dam-dataset}.sem_generic`;
create table `{dam-project}.{dam-dataset}.sem_generic` as
(
    SELECT distinct date('{start_dt}') as date, a.crn
    FROM
    (
        (
            SELECT distinct crn, time_utc, date
            FROM `{dam-project}.{dam-dataset}.sem_prod_clk` 
            where productGroupId is null
        ) a
        inner join
        (
            select crn, start_txn_time, sum(tot_amt_incld_gst-ass.tot_wow_dollar_incld_gst) as spend
            from 
            (
                select lylty_card_nbr, start_txn_time, prod_nbr, tot_amt_incld_gst, tot_wow_dollar_incld_gst, division_nbr, checkout_nbr
                from `gcp-wow-rwds-ai-safari-prod.wdp_tables.article_sales_summary`
                where division_nbr in (1005,1030) 
                and void_flag <> 'Y'  
                -- Changed time interval definition, to remove days before start_dt
                and Date(start_txn_time) >=  Date('{start_dt}')
                -- And restrict the interval to end_dt (previously it was open on the right)
                and Date(start_txn_time) <=  Date('{end_dt}')
            ) ass
            inner join
            (
                select prod_nbr, division_nbr 
                from `gcp-wow-rwds-ai-safari-prod.wdp_tables.article_master`
                where division_nbr in (1005,1030) 
            ) am on ass.prod_nbr=am.prod_nbr and ass.division_nbr=am.division_nbr
            inner join 
            (
                select lylty_card_nbr,crn -- change 5th
                from `gcp-wow-rwds-ai-safari-prod.wdp_tables.lylty_card_detail` 
                where lylty_card_status=1 and crn is not NULL and crn <> '0'
            ) as lcd on ass.lylty_card_nbr=lcd.lylty_card_nbr
            group by 1,2
        ) b on a.crn = b.crn 
                and timestamp_diff(b.start_txn_time, a.time_utc, SECOND) > 0
                and timestamp_diff(b.start_txn_time, a.time_utc, DAY) between 0 and 7
                
        left join `{dam-project}.{dam-dataset}.sem_direct` direct
        on a.crn = direct.crn
        left join `{dam-project}.{dam-dataset}.sem_indirect` indirect
        on a.crn = indirect.crn
        
    ) where  direct.crn is null and indirect.crn is null
    
);