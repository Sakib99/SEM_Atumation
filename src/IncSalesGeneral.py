from datetime import datetime
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from io import BytesIO, StringIO
import json
import math
import matplotlib.pyplot as plt
import numpy as np
from pynndescent import NNDescent
import os
import pandas as pd
import pytz
import sys
import time


tz = pytz.timezone('Australia/Sydney')


def load_config(config):
    """Returns a dictionary from the configuration."""

    with open(config, 'r') as f:
        spec = json.load(f)
    return spec


def exclude_inactive(df):
    """Removes all rows with INACTIVE."""

    mask = df['cvm'].isin(['INACTIVE', 'NA'])
    df = df[~mask].reset_index().drop(columns='index')
    return df


def exclude_lapsed(df):
    """Removes all rows with LAPSED."""

    mask = df['cvm'].isin(['LAPSED', 'NA'])
    df = df[~mask].reset_index().drop(columns='index')
    return df


def cvm_encoding(df):
    """Performs the encoding."""

    cvm = ['HVHIGH',
           'HVMED',
           'MVHIGH',
           'MVMEDA',
           'MVMEDB',
           'LVHFA',
           'LVHFB',
           'LVLF',
           'LVLFB',
           'LOW',
           'LAPSED',
           'INACTIVE']

    cvm_v = [4, 4, 3, 3, 3, 2, 2, 1, 1, 2.5, 0, 0]
    cvm_sow = [4, 2.5, 4, 3, 2, 4, 2.5, 4, 2.5, 1, 2.5, 1]  # according to the Quantium CVM model

    cvm_v_dict = dict(zip(cvm, cvm_v))
    cvm_sow_dict = dict(zip(cvm, cvm_sow))

    df['CVM_value'] = df['cvm'].map(cvm_v_dict)
    df['CVM_SOW'] = df['cvm'].map(cvm_sow_dict)
    missed_encodings = df['CVM_SOW'].isna().sum()
    print("{datetime}\tCVM encoding produced {numerr} error(s).".format(datetime=datetime.now(tz), numerr=missed_encodings))
    df = df.drop(columns='cvm')

    return df


def cvm_encoding_proc(target, control, encode_cvm=True, cat_vars=None, num_vars=None):
    """Encodes the CVM categorical variable to numerical variables."""

    if num_vars is None:
        num_vars = []
    if cat_vars is None:
        cat_vars = []
    if encode_cvm:
        if 'cvm' in cat_vars:
            target = cvm_encoding(target)
            control = cvm_encoding(control)

            cat_vars.remove('cvm')
            num_vars.append('CVM_value')
            num_vars.append('CVM_SOW')

        else:
            print("{datetime}\tCVM not in categorical variables".format(datetime=datetime.now(tz)))

    return target, control, cat_vars, num_vars


def NNDescent_matching(target=None,
                       control=None,
                       cat_vars=None,
                       num_vars=None,
                       n_neighbors=1,
                       n_neighbors_tree=50,
                       target_col_name='crn',
                       normalise_dist=True):
    """Perform Nearest Neighbor Descent.

    Combinations of the categorical variables are generated prior to matching
    to speed up the matching process. Instead of using the entire Control,
    only those in Control matching a particular combination of categorical
    variables are matched to the Target with these same categorical variables.
    """

    if isinstance(cat_vars, str):
        cat_vars = [cat_vars]
        print(cat_vars)

    if isinstance(num_vars, str):
        num_vars = [num_vars]
        print(num_vars)

    cat_combined = target.groupby(cat_vars).size().reset_index().drop(0, axis=1)
    print(cat_combined)
    n_groups = cat_combined.shape[0]

    df_out = pd.DataFrame()

    for i in range(n_groups):
        print("{datetime}\tStarting iteration {curitr} of {enditr}...".format(datetime=datetime.now(tz), curitr=i + 1, enditr=n_groups))

        entire_control = False

        cat_i = pd.DataFrame(columns=cat_vars)
        cat_i.loc[0] = cat_combined.loc[i, :]

        df_t_i = pd.merge(target, cat_i, how='inner', on=cat_vars).reset_index(drop=True)
        df_c_i = pd.merge(control, cat_i, how='inner', on=cat_vars).reset_index(drop=True)

        try:
            print("{datetime}\tUsing subset of Control...".format(datetime=datetime.now(tz)))
            print("{datetime}\tPerforming NDD...".format(datetime=datetime.now(tz)))
            tree = NNDescent(df_c_i[num_vars], n_neighbors=n_neighbors_tree, n_jobs=-1)
        except:
            print("{datetime}\tUsing subset of Control was not successful. Using the entire Control...".format(datetime=datetime.now(tz)))
            entire_control = True
            print("{datetime}\tPerforming NDD...".format(datetime=datetime.now(tz)))
            tree = NNDescent(control[num_vars], n_neighbors=n_neighbors_tree, n_jobs=-1)

        print("{datetime}\tFinished NDD.".format(datetime=datetime.now(tz)))

        print("{datetime}\tPerforming Target-Control query...".format(datetime=datetime.now(tz)))
        matching = tree.query(df_t_i[num_vars], k=n_neighbors)

        print("{datetime}\tFinished Target-Control query.".format(datetime=datetime.now(tz)))

        idx = pd.DataFrame(matching[0])

        if entire_control:
            for col in idx.columns:
                df_t_i[target_col_name + '_' + str(col)] = list(control.loc[idx[col], target_col_name])
        else:
            for col in idx.columns:
                df_t_i[target_col_name + '_' + str(col)] = list(df_c_i.loc[idx[col], target_col_name])

        dst = pd.DataFrame(matching[1], columns=['dist_' + str(x) for x in idx.columns])
        df_t_i = pd.concat([df_t_i, dst], axis=1)

        df_out = pd.concat([df_out, df_t_i], axis=0)

        print("{datetime}\tProgress: {curseg}/{endseg} segments processed.".format(datetime=datetime.now(tz), curseg=i + 1, endseg=n_groups))

    if normalise_dist:
        n_dim = len(num_vars)
        for dist_i in dst.columns:
            df_out[dist_i] = df_out[dist_i] / (n_dim ** 0.5)

    return df_out


def matching_prod(target=None,
                  control=None,
                  var_cat=None,
                  var_tpg=None,
                  var_bsk=None,
                  weights=None,
                  out_name='output',
                  n_neighbors=1,
                  target_col_name='crn',
                  normalise_dist=True,
                  out_dir='./',
                  out_vars=None,
                  out_table_header=None):
    """Matches Target with Control."""

    if var_bsk is None:
        var_bsk = []
    if var_cat is None:
        var_cat = []
    if var_tpg is None:
        var_tpg = []

    assert set(var_cat + var_tpg + var_bsk + [target_col_name]).issubset(target.columns)
    assert set(var_cat + var_tpg + var_bsk + [target_col_name]).issubset(control.columns)

    target['flag'] = 1
    control['flag'] = 0

    df = pd.concat([target, control], axis=0, sort=False)

    var_num = var_tpg + var_bsk

    for item in var_num:
        df[item] = df[item].rank(pct=True).fillna(0)

    if weights is not None:
        for k in weights.keys():
            print('Applying weight {} to variable {}.'.format(weights[k], k))
            df[k] = df[k] * weights[k]

    df_t_ = df.loc[df['flag'] == 1, [target_col_name] + var_cat + var_num].reset_index(drop=True)
    df_c_ = df.loc[df['flag'] == 0, [target_col_name] + var_cat + var_num].reset_index(drop=True)

    s = time.time()
    print("{datetime}\tBegin matching...".format(datetime=datetime.now(tz)))
    df_matched = NNDescent_matching(target=df_t_,
                                    control=df_c_,
                                    cat_vars=var_cat,
                                    num_vars=var_num,
                                    n_neighbors=n_neighbors,
                                    target_col_name=target_col_name,
                                    normalise_dist=normalise_dist)
    e = time.time()

    print("{datetime}\tProcess time: {prctim:.2f} minutes".format(datetime=datetime.now(tz), prctim=(e - s) / 60))

    if out_vars is None:
        out_vars = [x for x in df_matched.columns if target_col_name in x or 'dist' in x or 'offer_nbr' in x]

    print(df_matched.head())
    df_matched[out_vars].to_csv('{}{}'.format(out_dir, out_name), index=False, header=out_table_header)

    return df_matched, (e - s) / 60


class IncSales:
    
    def __init__(self, config, start_dt = None, end_dt = None, path = None):
    
        """Generic Incremental Sales class for calculating the incremental sales.
    
        Attributes:
            spec (string): contents of configuration file
            
        """
        
        print("{datetime}\t{method}\tFinding configurations at '{config}'...".format(datetime=datetime.now(tz), method='IncSales', config=config))
        try:
            self.spec = load_config(config)
        except:
            sys.exit("{datetime}\t{method}\tConfig error. Exiting...".format(datetime=datetime.now(tz), method='IncSales'))
        print("{datetime}\t{method}\tFound configurations.".format(datetime=datetime.now(tz), method='IncSales'))
        print("{datetime}\t{method}\tLoading configurations...".format(datetime=datetime.now(tz), method='IncSales'))
        if start_dt:
            self.spec['start_dt'] = start_dt
        if end_dt:
            self.spec['end_dt']   = end_dt
        self.folder = path

    def get_sql_query_location(self, query, folder='sql/'):
        return folder + query + '.sql'
        
        
    def prepare_sql(self, loc=None, sql_args=None):
        
        """Function to return formatted SQL code.
        
        Args:
            loc (string): file location of SQL code
            sql_args (dictionary): dictionary used to format the SQL code
            
        Returns:
            string: formatted SQL code
        
        """
        
        print("{datetime}\t{method}\tEntered 'prepare_sql' method.".format(datetime=datetime.now(tz), method='prepare_sql'))
        print("{datetime}\t{method}\tReading SQL file location {loc} with arguments:".format(datetime=datetime.now(tz), method='prepare_sql', loc=loc))
        
        for arg in sql_args:
            print("{datetime}\t{method}\t{argkey}: {argval}".format(datetime=datetime.now(tz), method='prepare_sql', argkey=arg, argval=sql_args[arg]))
            
        with open(loc, mode='r') as f:
            sql = f.read()
            
        print("{datetime}\t{method}\tReading SQL finished.".format(datetime=datetime.now(tz), method='prepare_sql'))
        
        return sql.format(**sql_args)
    
    
    def bq_only(self, project=None, credentials=None, sql=None):
        
        """Function to run a BigQuery script.
        
        Args:
            project (string): name of BigQuery project
            credentials (string): file location of credentials in JSON
            sql (string): SQL code
            
        Returns:
            None
        
        """
        
        client = bigquery.Client(project=project)
        
        print("{datetime}\t{method}\tEntered 'bq_only' method.".format(datetime=datetime.now(tz), method='bq_only'))
        print("{datetime}\t{method}\tProject set to '{project}'.".format(datetime=datetime.now(tz), method='bq_only', project=project))
        
        query_job = client.query(sql)
        print(query_job.result())
    
    def bq_to_bq(self, project=None, dataset=None, table=None, credentials=None, sql=None):
        
        """Function to create a BigQuery table using formatted SQL code.
        
        Args:
            project (string): name of BigQuery project
            dataset (string): name of BigQuery dataset
            table (string): name of BigQuery table
            credentials (string): file location of credentials in JSON
            sql (string): SQL code
            
        Returns:
            None
        
        """
        
        client = bigquery.Client(project=project)
        table_path = project + '.' + dataset + '.' + table
        
        print("{datetime}\t{method}\tEntered 'bq_to_bq' method.".format(datetime=datetime.now(tz), method='bq_to_bq'))
        
        print("{datetime}\t{method}\tProject set to '{project}'.".format(datetime=datetime.now(tz), method='bq_to_bq', project=project))
        print("{datetime}\t{method}\tDataset set to '{dataset}'.".format(datetime=datetime.now(tz), method='bq_to_bq', dataset=dataset))
        print("{datetime}\t{method}\tTable set to '{table}'.".format(datetime=datetime.now(tz), method='bq_to_bq', table=table))
        print("{datetime}\t{method}\tTable path set to '{table_path}'.".format(datetime=datetime.now(tz), method='bq_to_bq', table_path=table_path))
        print("{datetime}\t{method}\tDeleting table '{table}' at '{table_path}'...".format(datetime=datetime.now(tz), method='bq_to_bq', table=table, table_path=table_path))
        client.delete_table(table_path, not_found_ok=True)
        print("{datetime}\t{method}\tDeleted table '{table}' at '{table_path}'.".format(datetime=datetime.now(tz), method='bq_to_bq', table=table, table_path=table_path))

        job_config = bigquery.QueryJobConfig(destination=table_path)

        print("{datetime}\t{method}\tQuerying...".format(datetime=datetime.now(tz), method='bq_to_bq'))
        query_job = client.query(sql, job_config=job_config)
        query_job.result()
        print(query_job.result())
        print("hello")
        print("{datetime}\t{method}\tQuery results loaded to the table '{table}' at table path '{table_path}'".format(datetime=datetime.now(tz), method='bq_to_bq', table=table, table_path=table_path))

        return
    
    
    def check_bq(self, project=None, dataset=None, table=None, credentials=None):
        
        """Function to check if a BigQuery table exists.
        
        Args:
            project (string): name of BigQuery project
            dataset (string): name of BigQuery dataset
            table (string): name of BigQuery table
            credentials (string): file location of credentials in JSON
            
        Returns:
            bool: True if the BigQuery exists, False otherwise
        
        """
        
        
        table_path = project + '.' + dataset + '.' + table
        
        client = bigquery.Client(project=project)

        print("{datetime}\t{method}\tEntered 'check_bq' method.".format(datetime=datetime.now(tz), method='check_bq'))
        
        print("{datetime}\t{method}\tProject set to '{project}'.".format(datetime=datetime.now(tz), method='check_bq', project=project))
        print("{datetime}\t{method}\tDataset set to '{dataset}'.".format(datetime=datetime.now(tz), method='check_bq', dataset=dataset))
        print("{datetime}\t{method}\tTable set to '{table}'.".format(datetime=datetime.now(tz), method='check_bq', table=table))
        print("{datetime}\t{method}\tTable path set to '{table_path}'.".format(datetime=datetime.now(tz), method='check_bq', table_path=table_path))

        try:
            print("{datetime}\t{method}\tChecking existence of table '{table}' at table path '{table_path}'...".format(datetime=datetime.now(tz), method='check_bq', table=table, table_path=table_path))
            client.get_table(table_path)
            print("{datetime}\t{method}\tTable '{table}' at table path found '{table_path}'.".format(datetime=datetime.now(tz), method='check_bq', table=table, table_path=table_path))
            print("{datetime}\t{method}\tTest finished. Returning 'True'...".format(datetime=datetime.now(tz), method='check_bq'))
            return True
        except NotFound:
            print("{datetime}\t{method}\tTable path not found '{table_path}'.".format(datetime=datetime.now(tz), method='check_bq', table_path=table_path))
            print("{datetime}\t{method}\tTest finished. Returning 'False'...".format(datetime=datetime.now(tz), method='check_bq'))
            return False
        
        
    def bq_to_gcs(self, project=None, dataset=None, table=None, bucket=None, folder=None, filename=None, credentials=None):
        
        """Function to load BigQuery table to GCS.
        
        Args:
            project (string): name of BigQuery project
            dataset (string): name of BigQuery dataset
            table (string): name of BigQuery table
            bucket (string): name of Google Cloud Storage bucket
            folder (string): name of Google Cloud Storage folder
            filename (string): name of Google Cloud Storage file
            credentials (string): file location of credentials in JSON
            
        Returns:
            None
        
        """
        
        table_path = project + '.' + dataset + '.' + table
        destination_uri = "gs://{}/{}/{}".format(bucket, folder, filename)
        dataset_ref = bigquery.DatasetReference(project, dataset)
        table_ref = dataset_ref.table(table)
        
        client = bigquery.Client(project=project)

        print("{datetime}\t{method}\tEntered 'bq_to_gcs' method.".format(datetime=datetime.now(tz), method='bq_to_gcs'))
        
        print("{datetime}\t{method}\tProject set to '{project}'.".format(datetime=datetime.now(tz), method='bq_to_gcs', project=project))
        print("{datetime}\t{method}\tDataset set to '{dataset}'.".format(datetime=datetime.now(tz), method='bq_to_gcs', dataset=dataset))
        print("{datetime}\t{method}\tTable set to '{table}'.".format(datetime=datetime.now(tz), method='bq_to_gcs', table=table))
        print("{datetime}\t{method}\tTable path set to '{table_path}'.".format(datetime=datetime.now(tz), method='bq_to_gcs', table_path=table_path))

        print("{datetime}\t{method}\tBucket set to '{bucket}'.".format(datetime=datetime.now(tz), method='bq_to_gcs', bucket=bucket))
        print("{datetime}\t{method}\tFolder set to '{folder}'.".format(datetime=datetime.now(tz), method='bq_to_gcs', folder=folder))
        print("{datetime}\t{method}\tFilename set to '{filename}'.".format(datetime=datetime.now(tz), method='bq_to_gcs', filename=filename))

        print("{datetime}\t{method}\tLoading table '{table}' at '{table_path}' to '{destination_uri}'...".format(datetime=datetime.now(tz), method='bq_to_gcs', table=table, table_path=table_path, destination_uri=destination_uri))

        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            location="US",
        )
        extract_job.result()

        print("{datetime}\t{method}\tLoaded table '{table}' at '{table_path}' to '{destination_uri}'.".format(datetime=datetime.now(tz), method='bq_to_gcs', table=table, table_path=table_path, destination_uri=destination_uri))
        
        return
    
    
    def gcs_to_bq(self, project=None, dataset=None, table=None, bucket=None, folder=None, filename=None, credentials=None):
        
        """Function to load GCS file to BigQuery table.
        
        Args:
            project (string): name of BigQuery project
            dataset (string): name of BigQuery dataset
            table (string): name of BigQuery table
            bucket (string): name of Google Cloud Storage bucket
            folder (string): name of Google Cloud Storage folder
            filename (string): name of Google Cloud Storage file
            credentials (string): file location of credentials in JSON
            
        Returns:
            None
        
        """
        
        table_path = project + '.' + dataset + '.' + table
        source_uri = "gs://{}/{}/{}".format(bucket, folder, filename)
        dataset_ref = bigquery.DatasetReference(project, dataset)
        table_ref = dataset_ref.table(table)
        
        client = bigquery.Client(project=project)

        print("{datetime}\t{method}\tEntered 'gcs_to_bq' method.".format(datetime=datetime.now(tz), method='gcs_to_bq'))
        
        print("{datetime}\t{method}\tProject set to '{project}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', project=project))
        print("{datetime}\t{method}\tDataset set to '{dataset}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', dataset=dataset))
        print("{datetime}\t{method}\tTable set to '{table}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', table=table))
        print("{datetime}\t{method}\tTable path set to '{table_path}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', table_path=table_path))

        print("{datetime}\t{method}\tBucket set to '{bucket}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', bucket=bucket))
        print("{datetime}\t{method}\tFolder set to '{folder}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', folder=folder))
        print("{datetime}\t{method}\tFilename set to '{filename}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', filename=filename))

        dataset_ref = client.dataset(dataset)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.schema = [
            bigquery.SchemaField("ref_dt", "DATE"),
            bigquery.SchemaField("crn", "STRING"),
            bigquery.SchemaField("crn_0", "STRING"),
            bigquery.SchemaField("dist_0", "FLOAT")
        ]
        job_config.skip_leading_rows = 1

        job_config.source_format = bigquery.SourceFormat.CSV

        load_job = client.load_table_from_uri(
            source_uri,
            dataset_ref.table(table),
            job_config=job_config
        )

        print("{datetime}\t{method}\tStarting job {job_id}...".format(datetime=datetime.now(tz), method='gcs_to_bq', job_id=load_job.job_id))

        load_job.result()
        print("{datetime}\t{method}\tJob finished.".format(datetime=datetime.now(tz), method='gcs_to_bq'))

        destination_table = client.get_table(table_ref)
        print("{datetime}\t{method}\tLoaded {num_rows} rows.".format(datetime=datetime.now(tz), method='gcs_to_bq', num_rows=destination_table.num_rows))
        print(type(destination_table))
        return
    
    def gcs_to_bq_robis(self, project=None, dataset=None, table=None, bucket=None, folder=None, filename=None, credentials=None):
        
        """Function to load GCS file to BigQuery table.
        
        Args:
            project (string): name of BigQuery project
            dataset (string): name of BigQuery dataset
            table (string): name of BigQuery table
            bucket (string): name of Google Cloud Storage bucket
            folder (string): name of Google Cloud Storage folder
            filename (string): name of Google Cloud Storage file
            credentials (string): file location of credentials in JSON
            
        Returns:
            None
        
        """
        
        table_path = project + '.' + dataset + '.' + table
        source_uri = "gs://{}/{}/{}".format(bucket, folder, filename)
        dataset_ref = bigquery.DatasetReference(project, dataset)
        table_ref = dataset_ref.table(table)
        
        client = bigquery.Client(project=project)

        print("{datetime}\t{method}\tEntered 'gcs_to_bq' method.".format(datetime=datetime.now(tz), method='gcs_to_bq'))
        
        print("{datetime}\t{method}\tProject set to '{project}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', project=project))
        print("{datetime}\t{method}\tDataset set to '{dataset}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', dataset=dataset))
        print("{datetime}\t{method}\tTable set to '{table}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', table=table))
        print("{datetime}\t{method}\tTable path set to '{table_path}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', table_path=table_path))

        print("{datetime}\t{method}\tBucket set to '{bucket}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', bucket=bucket))
        print("{datetime}\t{method}\tFolder set to '{folder}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', folder=folder))
        print("{datetime}\t{method}\tFilename set to '{filename}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', filename=filename))

        dataset_ref = client.dataset(dataset)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.schema = [
            bigquery.SchemaField("ref_dt", "DATE"),
            bigquery.SchemaField("robis_visit_broad_tot_sale", "FLOAT"),
            bigquery.SchemaField("fbaarobis", "FLOAT"),
            bigquery.SchemaField("sales_factor", "FLOAT"),
            bigquery.SchemaField("robis_visit_net_inc_sale", "FLOAT"),
            bigquery.SchemaField("web_sessions", "FLOAT"),
            bigquery.SchemaField("app_sessions", "FLOAT"),
            bigquery.SchemaField("web_app_ratio", "FLOAT"),
            bigquery.SchemaField("web_inc_sales", "FLOAT"),
            bigquery.SchemaField("app_inc_sales", "FLOAT"),
            bigquery.SchemaField("total_web_app_inc_sales", "FLOAT")
        ]
        job_config.skip_leading_rows = 1

        job_config.source_format = bigquery.SourceFormat.CSV

        load_job = client.load_table_from_uri(
            source_uri,
            dataset_ref.table(table),
            job_config=job_config
        )

        print("{datetime}\t{method}\tStarting job {job_id}...".format(datetime=datetime.now(tz), method='gcs_to_bq', job_id=load_job.job_id))

        load_job.result()
        print("{datetime}\t{method}\tJob finished.".format(datetime=datetime.now(tz), method='gcs_to_bq'))

        destination_table = client.get_table(table_ref)
        print("{datetime}\t{method}\tLoaded {num_rows} rows.".format(datetime=datetime.now(tz), method='gcs_to_bq', num_rows=destination_table.num_rows))
        
        return
    
    def gcs_to_bq_sem(self, project=None, dataset=None, table=None, bucket=None, folder=None, filename=None, credentials=None):
        
        """Function to load GCS file to BigQuery table.
        
        Args:
            project (string): name of BigQuery project
            dataset (string): name of BigQuery dataset
            table (string): name of BigQuery table
            bucket (string): name of Google Cloud Storage bucket
            folder (string): name of Google Cloud Storage folder
            filename (string): name of Google Cloud Storage file
            credentials (string): file location of credentials in JSON
            
        Returns:
            None
        
        """
        
        table_path = project + '.' + dataset + '.' + table
        source_uri = "gs://{}/{}/{}".format(bucket, folder, filename)
        dataset_ref = bigquery.DatasetReference(project, dataset)
        table_ref = dataset_ref.table(table)
        
        client = bigquery.Client(project=project)

        print("{datetime}\t{method}\tEntered 'gcs_to_bq' method.".format(datetime=datetime.now(tz), method='gcs_to_bq'))
        
        print("{datetime}\t{method}\tProject set to '{project}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', project=project))
        print("{datetime}\t{method}\tDataset set to '{dataset}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', dataset=dataset))
        print("{datetime}\t{method}\tTable set to '{table}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', table=table))
        print("{datetime}\t{method}\tTable path set to '{table_path}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', table_path=table_path))

        print("{datetime}\t{method}\tBucket set to '{bucket}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', bucket=bucket))
        print("{datetime}\t{method}\tFolder set to '{folder}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', folder=folder))
        print("{datetime}\t{method}\tFilename set to '{filename}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', filename=filename))

        dataset_ref = client.dataset(dataset)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.schema = [
            bigquery.SchemaField("ref_dt", "DATE"),
            bigquery.SchemaField("sem_tot_sale", "FLOAT"),
            bigquery.SchemaField("sem_off_sales", "FLOAT"),
            bigquery.SchemaField("sem_onl_sales", "FLOAT"),
            bigquery.SchemaField("sem_inc_sale", "FLOAT"),
            bigquery.SchemaField("sem_btl_inc_sale", "FLOAT"),
            bigquery.SchemaField("sem_net_inc_sale", "FLOAT"),
            bigquery.SchemaField("match_rate", "FLOAT")
        ]
        job_config.skip_leading_rows = 1

        job_config.source_format = bigquery.SourceFormat.CSV

        load_job = client.load_table_from_uri(
            source_uri,
            dataset_ref.table(table),
            job_config=job_config
        )

        print("{datetime}\t{method}\tStarting job {job_id}...".format(datetime=datetime.now(tz), method='gcs_to_bq', job_id=load_job.job_id))

        load_job.result()
        print("{datetime}\t{method}\tJob finished.".format(datetime=datetime.now(tz), method='gcs_to_bq'))

        destination_table = client.get_table(table_ref)
        print("{datetime}\t{method}\tLoaded {num_rows} rows.".format(datetime=datetime.now(tz), method='gcs_to_bq', num_rows=destination_table.num_rows))
        
        return
    
    def gcs_to_bq_sem_bigw(self, project=None, dataset=None, table=None, bucket=None, folder=None, filename=None, credentials=None):
        
        """Function to load GCS file to BigQuery table.
        
        Args:
            project (string): name of BigQuery project
            dataset (string): name of BigQuery dataset
            table (string): name of BigQuery table
            bucket (string): name of Google Cloud Storage bucket
            folder (string): name of Google Cloud Storage folder
            filename (string): name of Google Cloud Storage file
            credentials (string): file location of credentials in JSON
            
        Returns:
            None
        
        """
        
        table_path = project + '.' + dataset + '.' + table
        source_uri = "gs://{}/{}/{}".format(bucket, folder, filename)
        dataset_ref = bigquery.DatasetReference(project, dataset)
        table_ref = dataset_ref.table(table)
        
        client = bigquery.Client(project=project)

        print("{datetime}\t{method}\tEntered 'gcs_to_bq' method.".format(datetime=datetime.now(tz), method='gcs_to_bq'))
        
        print("{datetime}\t{method}\tProject set to '{project}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', project=project))
        print("{datetime}\t{method}\tDataset set to '{dataset}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', dataset=dataset))
        print("{datetime}\t{method}\tTable set to '{table}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', table=table))
        print("{datetime}\t{method}\tTable path set to '{table_path}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', table_path=table_path))

        print("{datetime}\t{method}\tBucket set to '{bucket}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', bucket=bucket))
        print("{datetime}\t{method}\tFolder set to '{folder}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', folder=folder))
        print("{datetime}\t{method}\tFilename set to '{filename}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', filename=filename))

        dataset_ref = client.dataset(dataset)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.schema = [
            bigquery.SchemaField("ref_dt", "DATE"),
            bigquery.SchemaField("sem_tot_sale", "FLOAT"),
            bigquery.SchemaField("sem_off_sales", "FLOAT"),
            bigquery.SchemaField("sem_onl_sales", "FLOAT"),
            bigquery.SchemaField("sem_inc_sale", "FLOAT"),
            bigquery.SchemaField("sem_btl_inc_sale", "FLOAT"),
            bigquery.SchemaField("sem_net_inc_sale", "FLOAT"),
            bigquery.SchemaField("match_rate", "FLOAT")
        ]
        job_config.skip_leading_rows = 1

        job_config.source_format = bigquery.SourceFormat.CSV

        load_job = client.load_table_from_uri(
            source_uri,
            dataset_ref.table(table),
            job_config=job_config
        )

        print("{datetime}\t{method}\tStarting job {job_id}...".format(datetime=datetime.now(tz), method='gcs_to_bq', job_id=load_job.job_id))

        load_job.result()
        print("{datetime}\t{method}\tJob finished.".format(datetime=datetime.now(tz), method='gcs_to_bq'))

        destination_table = client.get_table(table_ref)
        print("{datetime}\t{method}\tLoaded {num_rows} rows.".format(datetime=datetime.now(tz), method='gcs_to_bq', num_rows=destination_table.num_rows))
        
        return
    
    def gcs_to_bq_dx(self, project=None, dataset=None, table=None, bucket=None, folder=None, filename=None, credentials=None):
        
        """Function to load GCS file to BigQuery table.
        
        Args:
            project (string): name of BigQuery project
            dataset (string): name of BigQuery dataset
            table (string): name of BigQuery table
            bucket (string): name of Google Cloud Storage bucket
            folder (string): name of Google Cloud Storage folder
            filename (string): name of Google Cloud Storage file
            credentials (string): file location of credentials in JSON
            
        Returns:
            None
        
        """
        
        table_path = project + '.' + dataset + '.' + table
        source_uri = "gs://{}/{}/{}".format(bucket, folder, filename)
        dataset_ref = bigquery.DatasetReference(project, dataset)
        table_ref = dataset_ref.table(table)
        
        client = bigquery.Client(project=project)

        print("{datetime}\t{method}\tEntered 'gcs_to_bq' method.".format(datetime=datetime.now(tz), method='gcs_to_bq'))
        
        print("{datetime}\t{method}\tProject set to '{project}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', project=project))
        print("{datetime}\t{method}\tDataset set to '{dataset}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', dataset=dataset))
        print("{datetime}\t{method}\tTable set to '{table}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', table=table))
        print("{datetime}\t{method}\tTable path set to '{table_path}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', table_path=table_path))

        print("{datetime}\t{method}\tBucket set to '{bucket}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', bucket=bucket))
        print("{datetime}\t{method}\tFolder set to '{folder}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', folder=folder))
        print("{datetime}\t{method}\tFilename set to '{filename}'.".format(datetime=datetime.now(tz), method='gcs_to_bq', filename=filename))

        dataset_ref = client.dataset(dataset)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.schema = [
            bigquery.SchemaField("ref_dt", "DATE"),
            bigquery.SchemaField("total_sales", "FLOAT"),
            bigquery.SchemaField("offline_sales", "FLOAT"),
            bigquery.SchemaField("online_sales", "FLOAT"),
            bigquery.SchemaField("online_sales_btl_inc_sales", "FLOAT"),
            bigquery.SchemaField("online_sales_net_inc_sales", "FLOAT"),
            bigquery.SchemaField("total_inc_sales", "FLOAT"),
            bigquery.SchemaField("btl_inc_sales", "FLOAT"),
            bigquery.SchemaField("net_inc_sales", "FLOAT"),
            bigquery.SchemaField("total_dx_inc_sales", "FLOAT"),
            bigquery.SchemaField("web_inc_sales_sum", "FLOAT"),
            bigquery.SchemaField("app_inc_sales_sum", "FLOAT"),
            bigquery.SchemaField("web_app_inc_sales_sum", "FLOAT"),
            bigquery.SchemaField("dx_final_sum", "FLOAT")
        ]
        job_config.skip_leading_rows = 1

        job_config.source_format = bigquery.SourceFormat.CSV

        load_job = client.load_table_from_uri(
            source_uri,
            dataset_ref.table(table),
            job_config=job_config
        )

        print("{datetime}\t{method}\tStarting job {job_id}...".format(datetime=datetime.now(tz), method='gcs_to_bq', job_id=load_job.job_id))

        load_job.result()
        print("{datetime}\t{method}\tJob finished.".format(datetime=datetime.now(tz), method='gcs_to_bq'))

        destination_table = client.get_table(table_ref)
        print("{datetime}\t{method}\tLoaded {num_rows} rows.".format(datetime=datetime.now(tz), method='gcs_to_bq', num_rows=destination_table.num_rows))
        
        return
    
    def gcs_to_df(self, project=None, bucket=None, folder=None, filename=None, credentials=None):
        
        """Function to load GCS file to local Pandas Dataframe.
        
        Args:
            project (string): name of BigQuery project
            bucket (string): name of Google Cloud Storage bucket
            folder (string): name of Google Cloud Storage folder
            filename (string): name of Google Cloud Storage file
            credentials (string): file location of credentials in JSON
            
        Returns:
            None
        
        """
        
        client = storage.Client(project=project)
        client_bucket = client.get_bucket(bucket)
        filepath = folder + '/' + filename

        print("{datetime}\t{method}\tEntered 'gcs_to_df' method.".format(datetime=datetime.now(tz), method='gcs_to_df'))
        
        print("{datetime}\t{method}\tProject set to '{project}'.".format(datetime=datetime.now(tz), method='gcs_to_df', project=project))
        print("{datetime}\t{method}\tBucket set to '{bucket}'.".format(datetime=datetime.now(tz), method='gcs_to_df', bucket=bucket))
        print("{datetime}\t{method}\tFolder set to '{folder}'.".format(datetime=datetime.now(tz), method='gcs_to_df', folder=folder))
        print("{datetime}\t{method}\tFilename set to '{filename}'.".format(datetime=datetime.now(tz), method='gcs_to_df', filename=filename))

        blob = client_bucket.blob(filepath)
        byte_stream = BytesIO()
        blob.download_to_file(byte_stream)
        byte_stream.seek(0)
        df = pd.read_csv(byte_stream)

        print("{datetime}\t{method}\tReturned {filename}.".format(datetime=datetime.now(tz), method='gcs_to_df', filename=filename))

        return df
    
    
    def local_to_gcs(self, project=None, bucket=None, folder=None, filename=None, loc=None, credentials=None):
        
        """Function to load local Pandas Dataframe to GCS file.
        
        Args:
            project (string): name of BigQuery project
            bucket (string): name of Google Cloud Storage bucket
            folder (string): name of Google Cloud Storage folder
            filename (string): name of Google Cloud Storage file
            loc (string): file location of SQL code
            credentials (string): file location of credentials in JSON
            
        Returns:
            None
        
        """
        
        client = storage.Client(project=project)
        client_bucket = client.get_bucket(bucket)
        filepath = folder + '/' + filename
        destination_uri = "gs://{}/{}/{}".format(bucket, folder, filename)

        print("{datetime}\t{method}\tEntered 'local_to_gcs' method.".format(datetime=datetime.now(tz), method='local_to_gcs'))
        
        print("{datetime}\t{method}\tProject set to '{project}'.".format(datetime=datetime.now(tz), method='local_to_gcs', project=project))
        print("{datetime}\t{method}\tBucket set to '{bucket}'.".format(datetime=datetime.now(tz), method='local_to_gcs', bucket=bucket))
        print("{datetime}\t{method}\tFolder set to '{folder}'.".format(datetime=datetime.now(tz), method='local_to_gcs', folder=folder))
        print("{datetime}\t{method}\tFilename set to '{filename}'.".format(datetime=datetime.now(tz), method='local_to_gcs', filename=filename))


        blob = client_bucket.blob(filepath)
        blob.upload_from_filename(loc)
        print(loc)

        print("{datetime}\t{method}\tFile '{loc}' uploaded to '{destination_uri}'.".format(datetime=datetime.now(tz), method='local_to_gcs', loc=loc, destination_uri=destination_uri))
        
        return
    
    
    def match(self, project=None, bucket=None, folder=None, filename=None, credentials=None, target=None, control=None, match=None, params=None):
        
        """Function to match Target and Control.
        'filename.csv' will be produced in the 'Output' folder.
        
        Args:
            project (string): name of BigQuery project
            bucket (string): name of Google Cloud Storage bucket
            folder (string): name of Google Cloud Storage folder
            filename (string): name of Google Cloud Storage file
            credentials (string): file location of credentials in JSON
            target (string): name of Target
            control (string): name of Control
            match (string): name of Match
            params (dictionary): parameters used for matching process
            
        Returns:
            out_table (dataframe): match table
            t (float): process time
        
        """
        
        
        """Perform the matching. """

        print("{datetime}\t{method}\t\tEntered 'match' method.".format(datetime=datetime.now(tz), method='match'))
        target = self.gcs_to_df(project=project, bucket=bucket, folder=folder, filename=target + '.csv', credentials=credentials)
        control = self.gcs_to_df(project=project, bucket=bucket, folder=folder, filename=control + '.csv', credentials=credentials)
        
        print(target.head())
        print(control.head())
        


        var_cat_ = params['var_cat']
        var_tpg_ = params['var_tpg']
        var_bsk_ = params['var_bsk']

        if not set(var_cat_ + var_tpg_ + var_bsk_).issubset(target.columns):
            missing_vars = ", ".join(list(set(var_cat_ + var_tpg_ + var_bsk_) - set(target.columns)))
            print("{datetime}\t{method}\t\tERROR: {missing_vars} not found in Target.".format(datetime=datetime.now(tz), method='match', missing_vars=missing_vars))
            return

        if not set(var_cat_ + var_tpg_ + var_bsk_).issubset(control.columns):
            missing_vars = ", ".join(list(set(var_cat_ + var_tpg_ + var_bsk_) - set(control.columns)))
            print("{datetime}\t{method}\t\tERROR: {missing_vars} not found in Control.".format(datetime=datetime.now(tz), method='match', missing_vars=missing_vars))
            return

        if 'cvm' in var_cat_:
            if ('INACTIVE' in target['cvm'].values):
                print("{datetime}\t{method}\t\tKeeping INACTIVE in control...".format(datetime=datetime.now(tz), method='match'))
            else:
                print("{datetime}\t{method}\t\tExcluding INACTIVE from control...".format(datetime=datetime.now(tz), method='match'))
                control = exclude_inactive(control)

            if ('LAPSED' in target['cvm'].values):
                print("{datetime}\t{method}\t\tKeeping LAPSED in control...".format(datetime=datetime.now(tz), method='match'))
            else:
                print("{datetime}\t{method}\t\tExcluding LAPSED from control...".format(datetime=datetime.now(tz), method='match'))
                control = exclude_lapsed(control)

        target, control, var_cat_, var_bsk_ = cvm_encoding_proc(target, control, encode_cvm=params['encode_cvm'], cat_vars=var_cat_, num_vars=var_bsk_)

        try:
            w_tpg = params['weights']['tpg']
            w_bsk = params['weights']['bsk']
            weights_ = dict(zip(var_tpg_ + var_bsk_, [w_tpg/len(var_tpg_)]*len(var_tpg_) + [w_bsk/len(var_bsk_)]*len(var_bsk_)))
            print("{datetime}\t{method}\t\tWeights successfully set.".format(datetime=datetime.now(tz), method='match'))
        except:
            weights_ = None

        print("{datetime}\t{method}\t\tPassing to the matching algorithm...".format(datetime=datetime.now(tz), method='match'))

        out_table, t = matching_prod(target=target,
                                               control=control,
                                               var_cat=var_cat_,
                                               var_tpg=var_tpg_,
                                               var_bsk=var_bsk_,
                                               weights=weights_,
                                               out_name=filename,
                                               n_neighbors=1,
                                               target_col_name='crn',
                                               normalise_dist=True,
                                               out_dir=params['out_dir'],
                                               out_vars=params['out_vars'],
                                               out_table_header=params['out_table_header_flag'])

        print("{datetime}\t{method}\t\tCompleted matching algorithm.".format(datetime=datetime.now(tz), method='match'))
        return out_table, t
    
    
    def plot_features(self, df=None, mode=None, save=None):
        
        """Function to plot features.
        Plots will be produced in the 'plot' folder if 'save' is set to 1.
        
        Args:
            df (dataframe): dataframe containing features
            mode (string): Target/Control or matched Target-Control
            save (boolean): save plots in 'plot' folder
            
        Returns:
            None
        
        """
        
#         cols = df.dtypes.to_dict()
        
#         print(cols)
#         print(len(cols))
        
        
        
#         for key in cols:
#             if key == 'crn':
#                 pass
#             else:

        df.hist()
        plt.show()
                
        
        return
        
#         for key in cols:
            
#             if cols[key] 
