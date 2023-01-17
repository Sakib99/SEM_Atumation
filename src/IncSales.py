from datetime import datetime
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from io import BytesIO, StringIO
import json
import math
import matplotlib.pyplot as plt
import os
import pandas as pd
import pytz
import time



from IncSalesGeneral import IncSales


tz = pytz.timezone('Australia/Sydney')


class IncSalesDX(IncSales):
    """ IncSalesDX class for obtaining DX incremental sales.
    
    Attributes:
            config (string): location of configuration file
            
    """
    
    def __init__(self, config='config/sem.json', start_dt = None, end_dt = None):
        
        IncSales.__init__(self, config, start_dt = start_dt, end_dt = end_dt)
    
    def get_sql_query_location(self, query, folder='sql/sem/'):
        return folder + query + '.sql'

    def calculate(self, project=None, dataset=None, table=None, start_dt=None, end_dt=None, match=None, credentials=None):
        
        print("{datetime}\t{method}\tEntered 'calculate' method.".format(datetime=datetime.now(tz), method='calculate'))
        
        print("{datetime}\t{method}\tAnalysis: DX.".format(datetime=datetime.now(tz), method='calculate'))

        print("{datetime}\t{method}\tCalculating DX...".format(datetime=datetime.now(tz), method='calculate'))

        
        if not project:
            project = self.spec['project']
            
        if not dataset:
            dataset = self.spec['dataset']
            
        if not table:
            table = self.spec['result']
            
        if not credentials:
            credentials = self.spec['credentials']
        
        loc = self.get_sql_query_location(table)
        sql_args = {'start_dt': start_dt, 'end_dt': end_dt, 'match': match}
        sql = self.prepare_sql(loc=loc, sql_args=sql_args)
        print(sql)
        self.bq_to_bq(project=project, dataset=dataset, table=table, credentials=credentials, sql=sql)
        
        return
        
    def automate(self, target=None, control=None, match=None, skip_list=None):
        
        """Function to run Incremental Sales.
        
        Args:
            target (string): name of Target
            control (string): name of Control
            match (string): name of Match
            skip_list (list): list of steps to skip
            
        Returns:
            None
        
        """
        
        if not skip_list:
            skip_list = []
        
        print("{datetime}\t{method}\tEntered 'automate' method.".format(datetime=datetime.now(tz), method='automate'))
        
        if not target:
            target = self.spec['target']
                
        if not control:
            control = self.spec['control']
                
        if not match:
            match = self.spec['match']
            
        if 'target' not in skip_list:
            print("{datetime}\t{method}\tAutomating Target...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(target)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['target'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Target...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'control' not in skip_list:
            print("{datetime}\t{method}\tAutomating Control...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(control)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['control'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Control...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'match' not in skip_list:
            print("{datetime}\t{method}\tAutomating Match...".format(datetime=datetime.now(tz), method='automate'))
            self.match(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'], target=self.spec['target'], control=self.spec['control'], match=self.spec['match'], params=self.spec)
            self.local_to_gcs(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', loc='output/' + self.spec['match'] + '.csv', credentials=self.spec['credentials'])
            self.gcs_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['match'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Match...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'calculate' not in skip_list:
            print("{datetime}\t{method}\tAutomating Calculate...".format(datetime=datetime.now(tz), method='automate'))
            self.calculate(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['result'], start_dt=self.spec['start_dt'], end_dt=self.spec['end_dt'], match=self.spec['match'], credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Calculate...".format(datetime=datetime.now(tz), method='automate'))

        if 'process1' not in skip_list:
            print("{datetime}\t{method}\tAutomating Process 1...".format(datetime=datetime.now(tz), method='automate'))
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['result'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['result'] + '.csv', credentials=self.spec['credentials'])
            dx_calculate = self.gcs_to_df(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['result'] + '.csv', credentials=self.spec['credentials'])
#             print(robis_calculate.head(999))
        else:
            print("{datetime}\t{method}\tSkipping Process 1...".format(datetime=datetime.now(tz), method='automate'))
            
        print(dx_calculate.head())

        if 'robis' not in skip_list:
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table='robis_inc_sales', bucket=self.spec['bucket'], folder=self.spec['folder'], filename='robis_inc_sales' + '.csv', credentials=self.spec['credentials'])
            robis_df = self.gcs_to_df(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename='robis_inc_sales' + '.csv', credentials=self.spec['credentials'])
            print(robis_df.head())
            robis_df_filtered = robis_df[(robis_df['ref_dt'] >= self.spec['start_dt']) & (robis_df['ref_dt'] < self.spec['end_dt'])]
            print(robis_df_filtered.head())
            
            web_inc_sales_sum = robis_df_filtered['web_inc_sales'].sum()
            app_inc_sales_sum = robis_df_filtered['app_inc_sales'].sum()
            web_app_inc_sales_sum = web_inc_sales_sum + app_inc_sales_sum
            
            dx_calculate['web_inc_sales_sum'] = web_inc_sales_sum
            dx_calculate['app_inc_sales_sum'] = app_inc_sales_sum
            dx_calculate['web_app_inc_sales_sum'] = web_app_inc_sales_sum
            
            dx_calculate['dx_final_sum'] = dx_calculate['total_dx_inc_sales'] + dx_calculate['web_app_inc_sales_sum']
            
            print(dx_calculate.head())

        out_dir = 'output/'
        out_name = 'sem_complete.csv'
        
        dx_calculate.to_csv('{}{}'.format(out_dir, out_name), index=False, header=True)
        self.local_to_gcs(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename='dx_complete' + '.csv', loc=out_dir + out_name, credentials=self.spec['credentials'])
        self.gcs_to_bq_dx(project=self.spec['project'], dataset=self.spec['dataset'], table='dx_inc_sales_pre', bucket=self.spec['bucket'], folder=self.spec['folder'], filename='dx_complete' + '.csv', credentials=self.spec['credentials'])
        
        if 'insert' not in skip_list:
            print("{datetime}\t{method}\tInserting DX results...".format(datetime=datetime.now(tz), method='automate'))
            loc = self.get_sql_query_location('insert_dx')
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_only(project=self.spec['project'], credentials=self.spec['credentials'], sql=sql)
        else:
            print("{datetime}\t{method}\tSkipping Insert...".format(datetime=datetime.now(tz), method='automate'))                     
        
        return


class IncSalesROBIS(IncSales):
    """ IncSalesROBIS class for obtaining ROBIS incremental sales.
    
    Attributes:
            config (string): location of configuration file
            
    """
    
    def __init__(self, config='analysis/robis.json', start_dt = None, end_dt = None, path = None):
        
        IncSales.__init__(self, config, start_dt = start_dt, end_dt = end_dt, path = path)
        
    def calculate(self, project=None, dataset=None, table=None, start_dt=None, end_dt=None, match=None, robis_view=None, robis_visit=None, robis_app_visit=None, robis_view_broad=None, robis_visit_broad=None, credentials=None):
        
        print("{datetime}\t{method}\tAnalysis: ROBIS.".format(datetime=datetime.now(tz), method='calculate'))

        print("{datetime}\t{method}\tCalculating ROBIS...".format(datetime=datetime.now(tz), method='calculate'))

        loc = self.get_sql_query_location(table)
        sql_args = {'start_dt': start_dt, 'end_dt': end_dt, 'match': match, 'robis_view': robis_view, 'robis_visit': robis_visit, 'robis_app_visit': robis_app_visit, 'robis_visit_broad': robis_visit_broad, 'robis_view_broad': robis_view_broad}
        sql = self.prepare_sql(loc=loc, sql_args=sql_args)
        self.bq_to_bq(project=project, dataset=dataset, table=table, credentials=credentials, sql=sql)
    
    def get_sql_query_location(self, query, folder='sql/robis/'):
        if self.folder:
            folder = self.folder
        return folder + query + '.sql'
    
    def plot(self, project=None, bucket=None, folder=None, target=None, control=None, match=None, mode=None, save=None, credentials=None):
        
        print("{datetime}\t{method}\tEntered 'plot' method.".format(datetime=datetime.now(tz), method='plot'))
        
        if not project:
            project = self.spec['project']
            
        if not bucket:
            bucket = self.spec['bucket']
            
        if not folder:
            folder = self.spec['folder']
            
        if not target:
            target = self.spec['target']
                
        if not control:
            control = self.spec['control']
                
        if not match:
            match = self.spec['match']
            
        if not credentials:
            credentials = self.spec['credentials']
            
        target_df = self.gcs_to_df(project=project, bucket=bucket, folder=folder, filename=target + '.csv', credentials=credentials)
        control_df = self.gcs_to_df(project=project, bucket=bucket, folder=folder, filename=control + '.csv', credentials=credentials)
        match_df = self.gcs_to_df(project=project, bucket=bucket, folder=folder, filename=match + '.csv', credentials=credentials)
        
        self.plot_features(df=target_df)
        self.plot_features(df=control_df)
        self.plot_features(df=match_df)
        
    def automate(self, target=None, robis_view=None, robis_visit=None, robis_app_visit=None, robis_visit_broad=None, robis_view_broad=None, control=None, match=None, skip_list=None):
        
        """Function to run Incremental Sales.
        
        Args:
            target (string): name of Target
            robis_view (string): name of Robis View
            robis_visit (string): name of Robis Visit
            control (string): name of Control
            match (string): name of Match
            skip_list (list): list of steps to skip
            
        Returns:
            None
        
        """
        
        if not skip_list:
            skip_list = []
        
        print("{datetime}\t{method}\tEntered 'automate' method.".format(datetime=datetime.now(tz), method='automate'))
        
        if not target:
            target = self.spec['target']
            
        if not robis_view:
            robis_view = self.spec['robis_view']
            
        if not robis_visit:
            robis_visit = self.spec['robis_visit']
            
        if not robis_app_visit:
            robis_app_visit = self.spec['robis_app_visit']
            
        if not robis_view_broad:
            robis_view_broad = self.spec['robis_view_broad']
            
        if not robis_visit_broad:
            robis_visit_broad = self.spec['robis_visit_broad']
                
        if not control:
            control = self.spec['control']
                
        if not match:
            match = self.spec['match']
            
        if 'target' not in skip_list:
            print("{datetime}\t{method}\tAutomating Target...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(target)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['target'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Target...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'robis_view' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS View...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = loc = self.get_sql_query_location(robis_view)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_view'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_view'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['robis_view'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping ROBIS View...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'robis_visit' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS Visit...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(robis_visit)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_visit'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_visit'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['robis_visit'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping ROBIS Visit...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'robis_app_visit' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS APP Visit...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt'], 'robis_visit':robis_visit}
            loc = self.get_sql_query_location(robis_app_visit)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_app_visit'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_app_visit'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['robis_app_visit'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping ROBIS APP Visit...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'robis_view_broad' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS View Broad...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(robis_view_broad)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_view_broad'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_view_broad'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['robis_view_broad'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping ROBIS View Broad...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'robis_visit_broad' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS Visit Broad...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(robis_visit_broad)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_visit_broad'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_visit_broad'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['robis_visit_broad'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping ROBIS Visit Broad...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'control' not in skip_list:
            print("{datetime}\t{method}\tAutomating Control...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(control)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['control'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Control...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'match' not in skip_list:
            print("{datetime}\t{method}\tAutomating Match...".format(datetime=datetime.now(tz), method='automate'))
            self.match(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'], target=self.spec['target'], control=self.spec['control'], match=self.spec['match'], params=self.spec)
            self.local_to_gcs(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', loc='output/' + self.spec['match'] + '.csv', credentials=self.spec['credentials'])
            self.gcs_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['match'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Match...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'match_spends' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS Match Spends...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location('robis_match_spends')
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_only(project=self.spec['project'], credentials=self.spec['credentials'], sql=sql)
        else:
            print("{datetime}\t{method}\tSkipping ROBIS Match Spends...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'calculate' not in skip_list:
            print("{datetime}\t{method}\tAutomating Calculate...".format(datetime=datetime.now(tz), method='automate'))
            self.calculate(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['result'], start_dt=self.spec['start_dt'], end_dt=self.spec['end_dt'], match=self.spec['match'], robis_view=self.spec['robis_view'], robis_visit=self.spec['robis_visit'], robis_app_visit=self.spec['robis_app_visit'], robis_visit_broad=self.spec['robis_visit_broad'], robis_view_broad=self.spec['robis_view_broad'], credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Calculate...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'process1' not in skip_list:
            print("{datetime}\t{method}\tAutomating Process 1...".format(datetime=datetime.now(tz), method='automate'))
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['result'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['result'] + '.csv', credentials=self.spec['credentials'])
            robis_calculate = self.gcs_to_df(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['result'] + '.csv', credentials=self.spec['credentials'])
#             print(robis_calculate.head(999))
        else:
            print("{datetime}\t{method}\tSkipping Process 1...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'web_sessions' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS WEB Sessions...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location('generate_robis_web_sessions')
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table='generate_robis_web_sessions', credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table='generate_robis_web_sessions', bucket=self.spec['bucket'], folder=self.spec['folder'], filename='generate_robis_web_sessions' + '.csv', credentials=self.spec['credentials'])
            web_sessions = self.gcs_to_df(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename='generate_robis_web_sessions' + '.csv', credentials=self.spec['credentials'])
#             print(web_sessions.head(999))
        else:
            print("{datetime}\t{method}\tSkipping ROBIS WEB Sessions...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'app_sessions' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS APP Sessions...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location('generate_robis_app_sessions')
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table='generate_robis_app_sessions', credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table='generate_robis_app_sessions', bucket=self.spec['bucket'], folder=self.spec['folder'], filename='generate_robis_app_sessions' + '.csv', credentials=self.spec['credentials'])
            app_sessions = self.gcs_to_df(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename='generate_robis_app_sessions' + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping ROBIS APP Sessions...".format(datetime=datetime.now(tz), method='automate'))

        
        robis_calculate = robis_calculate.set_index('ref_dt')
        
        robis_final_df = {}
        robis_final_df['ref_dt'] = self.spec['start_dt']
        robis_final_df['robis_visit_broad_tot_sale'] = robis_calculate.loc[[self.spec['start_dt']],["robis_visit_broad_tot_sale"]].values[0][0]
        
        robis_final_df['fbaarobis'] = 0

        if robis_final_df['robis_visit_broad_tot_sale'] < robis_final_df['fbaarobis']:
            robis_final_df['sales_factor'] = float(robis_final_df['fbaarobis']) / float(robis_final_df['robis_visit_broad_tot_sale'])
        elif robis_final_df['robis_visit_broad_tot_sale'] >= robis_final_df['fbaarobis']:
            robis_final_df['sales_factor'] = 1
    
        robis_final_df['robis_visit_net_inc_sale'] = robis_calculate.loc[[self.spec['start_dt']],["robis_visit_net_inc_sale"]].values[0][0]
        
        robis_final_df['robis_visit_net_inc_sale'] = float(robis_final_df['sales_factor']) * robis_final_df['robis_visit_net_inc_sale']
        
        robis_final_df['web_sessions'] = web_sessions.loc[[0],["f0_"]].values[0][0]
        robis_final_df['app_sessions'] = app_sessions.loc[[0],["f0_"]].values[0][0]
        
        robis_final_df['web_app_ratio'] = float(robis_final_df['app_sessions']) / (float(robis_final_df['web_sessions'])+ float(robis_final_df['app_sessions']))
        
        robis_final_df['web_inc_sales'] = (1-float(robis_final_df['web_app_ratio']))*robis_final_df['robis_visit_net_inc_sale']
        robis_final_df['app_inc_sales'] = float(robis_final_df['web_app_ratio']) * robis_final_df['robis_visit_net_inc_sale']
        
        robis_final_df['total_web_app_inc_sales'] = 0
        
        robis_final_df_2 = pd.DataFrame.from_dict([robis_final_df])
        
        
        print(robis_final_df_2)
        
        out_dir = 'output/'
        out_name = 'robis_complete.csv'
        
        robis_final_df_2.to_csv('{}{}'.format(out_dir, out_name), index=False, header=True)
        self.local_to_gcs(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename='robis_complete' + '.csv', loc=out_dir + out_name, credentials=self.spec['credentials'])
        self.gcs_to_bq_robis(project=self.spec['project'], dataset=self.spec['dataset'], table='robis_inc_sales_pre', bucket=self.spec['bucket'], folder=self.spec['folder'], filename='robis_complete' + '.csv', credentials=self.spec['credentials'])
        
        if 'insert' not in skip_list:
            print("{datetime}\t{method}\tInserting ROBIS results...".format(datetime=datetime.now(tz), method='automate'))
            loc = self.get_sql_query_location('insert_robis')
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_only(project=self.spec['project'], credentials=self.spec['credentials'], sql=sql)
        else:
            print("{datetime}\t{method}\tSkipping Insert...".format(datetime=datetime.now(tz), method='automate'))
        
        return
            

class IncSalesSEM(IncSales):
    """ IncSalesSEM class for obtaining SEM incremental sales.
    
    Attributes:
            config (string): location of configuration file
            
    """
    
    def __init__(self, config='config/sem.json', start_dt = None, end_dt = None, path = None):
        
        IncSales.__init__(self, config, start_dt = start_dt, end_dt = end_dt, path = path)
        
    def calculate(self, project, dataset, table, start_dt, end_dt, match, sem, credentials):
        
        print("{datetime}\t{method}\tAnalysis: SEM.".format(datetime=datetime.now(tz), method='calculate'))

        print("{datetime}\t{method}\tCalculating SEM...".format(datetime=datetime.now(tz), method='calculate'))

        loc = self.get_sql_query_location(table)
        print(start_dt,end_dt)
        print(sem)

        sql_args = {'start_dt': start_dt, 'end_dt': end_dt, 'match': match, 'sem': sem, 'safari-project': 'gcp-wow-rwds-ai-safari-prod', 'dx-dataset' : 'DX_dev', 'dam-project' : 'gcp-wow-rwds-ai-safari-prod', 'dam-dataset' : 'DX_dev'}
        sql = self.prepare_sql(loc=loc, sql_args=sql_args)
        print(sql)
        print(dataset)
        print(table)
        self.bq_to_bq(project=project, dataset=dataset, table=table, credentials=credentials, sql=sql)
    

    def get_sql_query_location(self, query, folder='sql/sem/'):
        if self.folder:
            folder = self.folder
        return folder + query + '.sql'
    
    def automate(self, target=None, sem=None, control=None, match=None, skip_list=None):
        
        """Function to run Incremental Sales.
        
        Args:
            target (string): name of Target
            sem (string): name of SEM
            control (string): name of Control
            match (string): name of Match
            skip_list (list): list of steps to skip
            
        Returns:
            None
        
        """
        
        if not skip_list:
            skip_list = []
        
        print("{datetime}\t{method}\tEntered 'automate' method.".format(datetime=datetime.now(tz), method='automate'))
        
        if not sem:
            sem = self.spec['sem']
        
        if not target:
            target = self.spec['target']
                
        if not control:
            control = self.spec['control']
                
        if not match:
            match = self.spec['match']
        
        if 'sem' not in skip_list:
            print("{datetime}\t{method}\tAutomating SEM...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(sem)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_only(project=self.spec['project'], credentials=self.spec['credentials'], sql=sql)
        else:
            print("{datetime}\t{method}\tSkipping SEM...".format(datetime=datetime.now(tz), method='automate'))
        
        if 'target' not in skip_list:
            print("{datetime}\t{method}\tAutomating Target...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(target)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], 
                          credentials=self.spec['credentials'], sql=sql)
            
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], \
                           bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['target'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Target...".format(datetime=datetime.now(tz), method='automate'))

        if 'control' not in skip_list:
            print("{datetime}\t{method}\tAutomating Control...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(control)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], \
                           bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['control'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Control...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'match' not in skip_list:
            print("{datetime}\t{method}\tAutomating Match...".format(datetime=datetime.now(tz), method='automate'))
            self.match(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], \
                       filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'], target=self.spec['target'], \
                       control=self.spec['control'], match=self.spec['match'], params=self.spec)
            
            self.local_to_gcs(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], \
                              filename=self.spec['match'] + '.csv', loc='output/' + self.spec['match'] + '.csv', credentials=self.spec['credentials'])
            
            self.gcs_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['match'], \
                           bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Match...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'calculate' not in skip_list:
            print("{datetime}\t{method}\tAutomating Calculate...".format(datetime=datetime.now(tz), method='automate'))
            
            self.calculate(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['result'], \
                           start_dt=self.spec['start_dt'], end_dt=self.spec['end_dt'], match=self.spec['match'], sem=self.spec['sem'], credentials=self.spec['credentials'])
            
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['result'], \
                           bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['result'] + '.csv', credentials=self.spec['credentials'])
            
            sem_calculate = self.gcs_to_df(project=self.spec['project'], bucket=self.spec['bucket'], \
                                           folder=self.spec['folder'], filename=self.spec['result'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Calculate...".format(datetime=datetime.now(tz), method='automate'))
            
        
        
        out_dir = 'output/'
        out_name = 'sem_complete.csv'
        
        sem_final_df_2.to_csv('{}{}'.format(out_dir, out_name), index=False, header=True)
        self.local_to_gcs(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename='sem_complete' + '.csv', loc=out_dir + out_name, credentials=self.spec['credentials'])
        self.gcs_to_bq_sem(project=self.spec['project'], dataset=self.spec['dataset'], table='sem_inc_sales_pre', bucket=self.spec['bucket'], folder=self.spec['folder'], filename='sem_complete' + '.csv', credentials=self.spec['credentials'])
            
        if 'insert' not in skip_list:
            print("{datetime}\t{method}\tInserting SEM results...".format(datetime=datetime.now(tz), method='automate'))
            loc = self.get_sql_query_location('insert_sem')
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_only(project=self.spec['project'], credentials=self.spec['credentials'], sql=sql)
        else:
            print("{datetime}\t{method}\tSkipping Insert...".format(datetime=datetime.now(tz), method='automate'))
        
        return
    

class IncSalesSEM_BIGW(IncSales):
    """ IncSalesSEM_BIGW class for obtaining SEM_BIGW incremental sales.
    
    Attributes:
            config (string): location of configuration file
            
    """
    
    def __init__(self, config='analysis/sem_bigw.json', start_dt = None, end_dt = None):
        
        IncSales.__init__(self, config, start_dt = start_dt, end_dt = end_dt)

    def get_sql_query_location(self, query, folder='sql/sem_bigw/'):
        return folder + query + '.sql'
        
    def calculate(self, project=None, dataset=None, table=None, start_dt=None, end_dt=None, match=None, sem_bigw=None, credentials=None):
        
        print("{datetime}\t{method}\tAnalysis: SEM_BIGW.".format(datetime=datetime.now(tz), method='calculate'))

        print("{datetime}\t{method}\tCalculating SEM_BIGW...".format(datetime=datetime.now(tz), method='calculate'))

#         loc = self.get_sql_query_location(table)
#         sql_args = {'start_dt': start_dt, 'end_dt': end_dt, 'match': match, 'sem_bigw': sem_bigw}
#         sql = self.prepare_sql(loc=loc, sql_args=sql_args)
#         self.bq_to_bq(project=project, dataset=dataset, table=table, credentials=credentials, sql=sql)
        
        loc = self.get_sql_query_location(table)
        sql_args = {'start_dt': start_dt, 'end_dt': end_dt, 'match': match, 'sem_bigw': sem_bigw, 'safari-project': 'gcp-wow-rwds-ai-safari-prod', 'dx-dataset' : 'DX', 'dam-project' : 'gcp-wow-rwds-ai-safari-prod', 'dam-dataset' : 'DX'}
        sql = self.prepare_sql(loc=loc, sql_args=sql_args)
        self.bq_to_bq(project=project, dataset=dataset, table=table, credentials=credentials, sql=sql)
        
    def automate(self, target=None, sem_bigw=None, control=None, match=None, skip_list=None):
        
        """Function to run Incremental Sales.
        
        Args:
            target (string): name of Target
            sem_bigw (string): name of SEM_BIGW
            control (string): name of Control
            match (string): name of Match
            skip_list (list): list of steps to skip
            
        Returns:
            None
        
        """
        
        if not skip_list:
            skip_list = []
        
        print("{datetime}\t{method}\tEntered 'automate' method.".format(datetime=datetime.now(tz), method='automate'))
        
        if not sem_bigw:
            sem_bigw = self.spec['sem_bigw']
        
        if not target:
            target = self.spec['target']
                
        if not control:
            control = self.spec['control']
                
        if not match:
            match = self.spec['match']
        
        if 'sem_bigw' not in skip_list:
            print("{datetime}\t{method}\tAutomating SEM_BIGW...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(sem_bigw)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_only(project=self.spec['project'], credentials=self.spec['credentials'], sql=sql)
        else:
            print("{datetime}\t{method}\tSkipping Target...".format(datetime=datetime.now(tz), method='automate'))
        
        if 'target' not in skip_list:
            print("{datetime}\t{method}\tAutomating Target...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(target)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['target'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Target...".format(datetime=datetime.now(tz), method='automate'))

        if 'control' not in skip_list:
            print("{datetime}\t{method}\tAutomating Control...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = self.get_sql_query_location(control)
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['control'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Control...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'match' not in skip_list:
            print("{datetime}\t{method}\tAutomating Match...".format(datetime=datetime.now(tz), method='automate'))
            self.match(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'], target=self.spec['target'], control=self.spec['control'], match=self.spec['match'], params=self.spec)
            self.local_to_gcs(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', loc='output/' + self.spec['match'] + '.csv', credentials=self.spec['credentials'])
            self.gcs_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['match'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Match...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'calculate' not in skip_list:
            print("{datetime}\t{method}\tAutomating Calculate...".format(datetime=datetime.now(tz), method='automate'))
            self.calculate(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['result'], start_dt=self.spec['start_dt'], end_dt=self.spec['end_dt'], match=self.spec['match'], sem_bigw=self.spec['sem_bigw'], credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Calculate...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'process1' not in skip_list:
            print("{datetime}\t{method}\tAutomating Process 1...".format(datetime=datetime.now(tz), method='automate'))
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['result'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['result'] + '.csv', credentials=self.spec['credentials'])
            sem_bigw_calculate = self.gcs_to_df(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['result'] + '.csv', credentials=self.spec['credentials'])
#             print(robis_calculate.head(999))
        else:
            print("{datetime}\t{method}\tSkipping Process 1...".format(datetime=datetime.now(tz), method='automate'))
            
        
        if 'insert' not in skip_list:
            print("{datetime}\t{method}\tInserting SEM_BIGW results...".format(datetime=datetime.now(tz), method='automate'))
            loc = self.get_sql_query_location('insert_sem_bigw')
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_only(project=self.spec['project'], credentials=self.spec['credentials'], sql=sql)
        else:
            print("{datetime}\t{method}\tSkipping Insert...".format(datetime=datetime.now(tz), method='automate'))
        
        return
    
    
class IncSalesWP(IncSales):
    """ IncSalesWP class for obtaining Weekly Picks incremental sales.
    
    Attributes:
            config (string): location of configuration file
            
    """
    
    def __init__(self, config='analysis/wp.json'):
        
        IncSales.__init__(self, config)
        
    def calculate(self, project=None, dataset=None, table=None, start_dt=None, end_dt=None, match=None, credentials=None):
        
        print("{datetime}\t{method}\tAnalysis: Weekly Picks.".format(datetime=datetime.now(tz), method='calculate'))

        print("{datetime}\t{method}\tCalculating Weekly Picks...".format(datetime=datetime.now(tz), method='calculate'))

        loc = 'sql/' + table + '.sql'
        sql_args = {'start_dt': start_dt, 'end_dt': end_dt, 'match': match}
        sql = self.prepare_sql(loc=loc, sql_args=sql_args)
        self.bq_to_bq(project=project, dataset=dataset, table=table, credentials=credentials, sql=sql)
        
    def automate(self, target=None, sem=None, control=None, match=None, skip_list=None):
        
        """Function to run Incremental Sales.
        
        Args:
            target (string): name of Target
            wp (string): name of Weekly Picks
            control (string): name of Control
            match (string): name of Match
            skip_list (list): list of steps to skip
            
        Returns:
            None
        
        """
        
        if not skip_list:
            skip_list = []
        
        print("{datetime}\t{method}\tEntered 'automate' method.".format(datetime=datetime.now(tz), method='automate'))
        
        if not target:
            target = self.spec['target']
                
        if not control:
            control = self.spec['control']
                
        if not match:
            match = self.spec['match']
        
        if 'target' not in skip_list:
            print("{datetime}\t{method}\tAutomating Target...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = 'sql/' + target + '.sql'
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['target'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Target...".format(datetime=datetime.now(tz), method='automate'))

        if 'control' not in skip_list:
            print("{datetime}\t{method}\tAutomating Control...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = 'sql/' + control + '.sql'
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['control'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Control...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'match' not in skip_list:
            print("{datetime}\t{method}\tAutomating Match...".format(datetime=datetime.now(tz), method='automate'))
            self.match(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'], target=self.spec['target'], control=self.spec['control'], match=self.spec['match'], params=self.spec)
            self.local_to_gcs(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', loc='output/' + self.spec['match'] + '.csv', credentials=self.spec['credentials'])
            self.gcs_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['match'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Match...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'calculate' not in skip_list:
            print("{datetime}\t{method}\tAutomating Calculate...".format(datetime=datetime.now(tz), method='automate'))
            self.calculate(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['result'], start_dt=self.spec['start_dt'], end_dt=self.spec['end_dt'], match=self.spec['match'], credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Calculate...".format(datetime=datetime.now(tz), method='automate'))
        
        return
    
    
class IncSalesROBIS2(IncSales):
    """ IncSalesROBIS2 class for obtaining ROBIS incremental sales.
    
    Attributes:
            config (string): location of configuration file
            
    """
    
    def __init__(self, config='analysis/robis2.json'):
        
        IncSales.__init__(self, config)
        
    def calculate(self, project=None, dataset=None, table=None, start_dt=None, end_dt=None, match=None, robis_web=None, robis_app=None, credentials=None):
        
        print("{datetime}\t{method}\tAnalysis: ROBIS2.".format(datetime=datetime.now(tz), method='calculate'))

        print("{datetime}\t{method}\tCalculating ROBIS2...".format(datetime=datetime.now(tz), method='calculate'))

        loc = 'sql/' + table + '.sql'
        sql_args = {'start_dt': start_dt, 'end_dt': end_dt, 'match': match, 'robis_web': robis_web, 'robis_app': robis_app}
        sql = self.prepare_sql(loc=loc, sql_args=sql_args)
        self.bq_to_bq(project=project, dataset=dataset, table=table, credentials=credentials, sql=sql)
        
    def automate(self, target=None, robis_web=None, robis_app=None, control=None, match=None, skip_list=None):
        
        """Function to run Incremental Sales.
        
        Args:
            target (string): name of Target
            robis_view (string): name of Robis View
            robis_visit (string): name of Robis Visit
            control (string): name of Control
            match (string): name of Match
            skip_list (list): list of steps to skip
            
        Returns:
            None
        
        """
        
        if not skip_list:
            skip_list = []
        
        print("{datetime}\t{method}\tEntered 'automate' method.".format(datetime=datetime.now(tz), method='automate'))
        
        if not target:
            target = self.spec['target']
            
        if not robis_web:
            robis_web = self.spec['robis_web']
            
        if not robis_app:
            robis_app = self.spec['robis_app']
                
        if not control:
            control = self.spec['control']
                
        if not match:
            match = self.spec['match']
            
        if 'target' not in skip_list:
            print("{datetime}\t{method}\tAutomating Target...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = 'sql/' + target + '.sql'
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['target'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Target...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'robis_web' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS Web...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = 'sql/' + robis_web + '.sql'
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_web'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_web'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['robis_web'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping ROBIS Web...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'robis_app' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS App...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = 'sql/' + robis_app + '.sql'
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_app'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_app'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['robis_app'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping ROBIS App...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'control' not in skip_list:
            print("{datetime}\t{method}\tAutomating Control...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = 'sql/' + control + '.sql'
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['control'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Control...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'match' not in skip_list:
            print("{datetime}\t{method}\tAutomating Match...".format(datetime=datetime.now(tz), method='automate'))
            self.match(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'], target=self.spec['target'], control=self.spec['control'], match=self.spec['match'], params=self.spec)
            self.local_to_gcs(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', loc='output/' + self.spec['match'] + '.csv', credentials=self.spec['credentials'])
            self.gcs_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['match'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Match...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'calculate' not in skip_list:
            print("{datetime}\t{method}\tAutomating Calculate...".format(datetime=datetime.now(tz), method='automate'))
            self.calculate(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['result'], start_dt=self.spec['start_dt'], end_dt=self.spec['end_dt'], match=self.spec['match'], robis_web=self.spec['robis_web'], robis_app=self.spec['robis_app'], credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Calculate...".format(datetime=datetime.now(tz), method='automate'))
        
        return
    

class IncSalesROBISBSC(IncSales):
    """ IncSalesROBISBSC class for obtaining ROBIS incremental sales for Browse, Search, Catalogue.
    
    Attributes:
            config (string): location of configuration file
            
    """
    
    def __init__(self, config='analysis/robisbsc.json'):
        
        IncSales.__init__(self, config)
        
    def calculate(self, project=None, dataset=None, table=None, start_dt=None, end_dt=None, match=None, robis_browse=None, robis_search=None, robis_catalogue=None, credentials=None):
        
        print("{datetime}\t{method}\tAnalysis: ROBISBSC.".format(datetime=datetime.now(tz), method='calculate'))

        print("{datetime}\t{method}\tCalculating ROBISBSC...".format(datetime=datetime.now(tz), method='calculate'))

        loc = 'sql/' + table + '.sql'
        sql_args = {'start_dt': start_dt, 'end_dt': end_dt, 'match': match, 'robis_browse': robis_browse, 'robis_search': robis_search, 'robis_catalogue': robis_catalogue}
        sql = self.prepare_sql(loc=loc, sql_args=sql_args)
        self.bq_to_bq(project=project, dataset=dataset, table=table, credentials=credentials, sql=sql)
        
    def plot(self, project=None, bucket=None, folder=None, target=None, control=None, match=None, mode=None, save=None, credentials=None):
        
        print("{datetime}\t{method}\tEntered 'plot' method.".format(datetime=datetime.now(tz), method='plot'))
        
        if not project:
            project = self.spec['project']
            
        if not bucket:
            bucket = self.spec['bucket']
            
        if not folder:
            folder = self.spec['folder']
            
        if not target:
            target = self.spec['target']
                
        if not control:
            control = self.spec['control']
                
        if not match:
            match = self.spec['match']
            
        if not credentials:
            credentials = self.spec['credentials']
            
        target_df = self.gcs_to_df(project=project, bucket=bucket, folder=folder, filename=target + '.csv', credentials=credentials)
        control_df = self.gcs_to_df(project=project, bucket=bucket, folder=folder, filename=control + '.csv', credentials=credentials)
        match_df = self.gcs_to_df(project=project, bucket=bucket, folder=folder, filename=match + '.csv', credentials=credentials)
        
        self.plot_features(df=target_df)
        self.plot_features(df=control_df)
        self.plot_features(df=match_df)
        
    def automate(self, target=None, robis_browse=None, robis_search=None, robis_catalogue=None, control=None, match=None, skip_list=None):
        
        """Function to run Incremental Sales.
        
        Args:
            target (string): name of Target
            robis_view (string): name of Robis View
            robis_visit (string): name of Robis Visit
            control (string): name of Control
            match (string): name of Match
            skip_list (list): list of steps to skip
            
        Returns:
            None
        
        """
        
        if not skip_list:
            skip_list = []
        
        print("{datetime}\t{method}\tEntered 'automate' method.".format(datetime=datetime.now(tz), method='automate'))
        
        if not target:
            target = self.spec['target']
            
        if not robis_browse:
            robis_browse = self.spec['robis_browse']
            
        if not robis_search:
            robis_search = self.spec['robis_search']
            
        if not robis_catalogue:
            robis_catalogue = self.spec['robis_catalogue']
                
        if not control:
            control = self.spec['control']
                
        if not match:
            match = self.spec['match']
            
        if 'target' not in skip_list:
            print("{datetime}\t{method}\tAutomating Target...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = 'sql/' + target + '.sql'
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['target'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['target'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Target...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'robis_browse' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS Browse...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = 'sql/' + robis_browse + '.sql'
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_browse'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_browse'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['robis_browse'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping ROBIS Browse...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'robis_search' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS Search...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = 'sql/' + robis_search + '.sql'
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_search'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_search'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['robis_search'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping ROBIS Search...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'robis_catalogue' not in skip_list:
            print("{datetime}\t{method}\tAutomating ROBIS Catalogue...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = 'sql/' + robis_catalogue + '.sql'
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_catalogue'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['robis_catalogue'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['robis_catalogue'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping ROBIS Catalogue...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'control' not in skip_list:
            print("{datetime}\t{method}\tAutomating Control...".format(datetime=datetime.now(tz), method='automate'))
            sql_args = {'start_dt': self.spec['start_dt'], 'end_dt': self.spec['end_dt']}
            loc = 'sql/' + control + '.sql'
            sql = self.prepare_sql(loc=loc, sql_args=sql_args)
            self.bq_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], credentials=self.spec['credentials'], sql=sql)
            self.bq_to_gcs(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['control'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['control'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Control...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'match' not in skip_list:
            print("{datetime}\t{method}\tAutomating Match...".format(datetime=datetime.now(tz), method='automate'))
            self.match(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'], target=self.spec['target'], control=self.spec['control'], match=self.spec['match'], params=self.spec)
            self.local_to_gcs(project=self.spec['project'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', loc='output/' + self.spec['match'] + '.csv', credentials=self.spec['credentials'])
            self.gcs_to_bq(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['match'], bucket=self.spec['bucket'], folder=self.spec['folder'], filename=self.spec['match'] + '.csv', credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Match...".format(datetime=datetime.now(tz), method='automate'))
            
        if 'calculate' not in skip_list:
            print("{datetime}\t{method}\tAutomating Calculate...".format(datetime=datetime.now(tz), method='automate'))
            self.calculate(project=self.spec['project'], dataset=self.spec['dataset'], table=self.spec['result'], start_dt=self.spec['start_dt'], end_dt=self.spec['end_dt'], match=self.spec['match'], robis_browse=self.spec['robis_browse'], robis_search=self.spec['robis_search'], robis_catalogue=self.spec['robis_catalogue'], credentials=self.spec['credentials'])
        else:
            print("{datetime}\t{method}\tSkipping Calculate...".format(datetime=datetime.now(tz), method='automate'))
        
        return