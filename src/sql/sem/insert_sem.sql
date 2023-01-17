
delete from `{safari-project}.{dx-dataset}.sem_inc_sales` where ref_dt = '{start_dt}';
INSERT `{safari-project}.{dx-dataset}.sem_inc_sales`
SELECT *, current_timestamp() as last_updated
FROM `{safari-project}.{dx-dataset}.calculate_sem`