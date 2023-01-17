from datetime import datetime
import time
import argparse
import pytz
import pandas as pd
import re
import sys
from IncSalesGeneral import IncSales
from IncSales import IncSalesSEM
tz = pytz.timezone('Australia/Sydney')

def check_format(date_,day):
    try:
        reg=r"[0-9]{4}[\-][0-9]{2}[\-][0-9]{2}"
        match = re.search(reg,date_)
        print(match.group())
        check_date(date_,day)
    except:

        print("time data  " +date_+ "  does not match format '%y-%m-%d'")
        sys.exit()
        


def check_date(date_,day):
    d=pd.to_datetime(date_)
    weekday = d.date().weekday()
    
    if(weekday==0 and day=='mon'):
            print("Entered Correct Date")
    elif(weekday!=0 and day=='mon'):
            print(" Please Enter Correct Date")
            sys.exit()        
    elif(weekday==6 and day=='sun'):
            print("Entered Correct Date")
    else:
        print(" Please Enter Correct Date")
        sys.exit()

def check_env(env_):
    try:
        envs=['dev','prod']
        if(env_ in envs):
            print("Entered valid environment")
        else:
            print("Invalid Environment")
            sys.exit()
    except:
        print("invalid env name")
tz = pytz.timezone('Australia/Sydney')

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        # start_dt = today - datetime.timedelta(days=today.weekday(), weeks=1)
        "start_dt", type=str, help="Start Date: Monday of the previous week"
    )
    parser.add_argument(
        # end_dt = start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')
        "end_dt", type=str, help="End Date: Sunday of the previous week"
    )
    
    parser.add_argument(
        "dataset", type=str, help="SQL file name"
    )
    parser.add_argument(
        "env_dir", type=str, help="Environemnt Directory name"
    )
    args = parser.parse_args()
    check_format(args.start_dt,'mon')
    check_format(args.end_dt,'sun')
    check_env(args.env_dir)
    obj = IncSalesSEM(config='config/'+args.env_dir+'/sem.json',start_dt=args.start_dt, end_dt=args.end_dt, path = 'sql/sem/')


    sem = obj.spec['sem'] # generate_sem
    control = obj.spec['control'] # generate_target_sem

    print("{datetime}\t{method}\tAutomating control...".format(datetime=datetime.now(tz), method='automate'))
    sql_args = {'start_dt': args.start_dt, 'end_dt': args.end_dt, 'dam-project' : obj.spec['project'], 'dam-dataset' : args.dataset}
    loc =  obj.get_sql_query_location(control)
    sql = obj.prepare_sql(loc=loc, sql_args=sql_args)
    print(sql)
    obj.bq_to_bq(project=obj.spec['project'], dataset=args.dataset, table=obj.spec['control'], credentials="none", sql=sql)
    print("Big_Query_For the control SEM")
    obj.bq_to_gcs(project=obj.spec['project'], dataset=args.dataset, table=obj.spec['control'], bucket=obj.spec['bucket'], folder=obj.spec['folder'], filename=obj.spec['control'] + '.csv', credentials="none")




if __name__ == "__main__":
    main()