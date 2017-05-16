# Django specific settings
import os
import pandas as pd

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
# Ensure settings are read
from django.core.wsgi import get_wsgi_application

application = get_wsgi_application()

from datetime import datetime
from dateutil import parser
from django.core.exceptions import ObjectDoesNotExist
from django.db import IntegrityError
import shutil
import pandas as pd
from activity.models import (List, Campaign, Customer,
                             Activity, ResponsysFile)

from sqlalchemy import create_engine

email_data_engine = create_engine(
  'postgresql://xxxxxxxx.amazonaws.com:5432')



class CustomerAttribute:

    def __init__(self):
        self.db = email_data_engine
        self.read_data()

        raw_activity_df = self.raw_activity_df.loc[(self.raw_activity_df['action'] == 4) | (self.raw_activity_df['action'] == 5)]
        riid_list = raw_activity_df['RIID'].unique()
        print ('', riid_list)
        #unique_opens_raw = unique_opens_raw.sort_values(['campaign_id', 'RIID'], ascending=True)
        #unique_opens_df = unique_opens_raw.drop_duplicates(['campaign_id', 'RIID'], keep='first')
        i = 0
        for riid in riid_list:
            i = i + 1
            print (i)
            #self.update_avg_opens(riid)
            self.update_avg_clicks(riid)


    def read_data(self):
        sql_activity = 'select "RIID", action, timestamp, campaign_id, list_id, customer_id ' \
                       'from email_data.activity_activity where action in (1, 4, 5)'
        self.raw_activity_df = pd.read_sql(sql_activity, self.db)
        print (self.raw_activity_df)


    def update_avg_opens(self, riid):
        print ('riid : ', riid)
        try:
            total_sent_df = self.raw_activity_df[
                (self.raw_activity_df['action'] == 1) & (self.raw_activity_df['RIID'] == riid)]
            total_sent_count = len(total_sent_df)

            unique_opens_raw = self.raw_activity_df[
                                (self.raw_activity_df['action'] == 4) & (self.raw_activity_df['RIID'] == riid)
                                & (self.raw_activity_df['campaign_id'].isin(total_sent_df['campaign_id']))]
            unique_opens_raw = unique_opens_raw.sort_values(['campaign_id', 'RIID'], ascending=True)
            unique_opens_df = unique_opens_raw.drop_duplicates(['campaign_id', 'RIID'], keep='first')
            total_unique_opens_count = len(unique_opens_df)
            avg_open = round(float(total_unique_opens_count) / total_sent_count, 4)
            print('total_unique_opens_count', total_unique_opens_count)
            print('total_sent_df_count', total_sent_count)
            print('avg_open', avg_open)

            #if Customer.objects.filter(RIID=riid).exists():
                #print('I am herer', riid)
            Customer.objects.filter(RIID=riid).update(avg_open=avg_open)

        except Exception as e:
            print (e)


    def update_avg_clicks(self, riid):
        print('riid : ', riid)
        try:
            total_sent_df = self.raw_activity_df[
                (self.raw_activity_df['action'] == 1) & (self.raw_activity_df['RIID'] == riid)]
            total_sent_count = len(total_sent_df)

            unique_clicks_raw = self.raw_activity_df[
                (self.raw_activity_df['action'] == 5) & (self.raw_activity_df['RIID'] == riid)
                & (self.raw_activity_df['campaign_id'].isin(total_sent_df['campaign_id']))]
            unique_clicks_raw = unique_clicks_raw.sort_values(['campaign_id', 'RIID'], ascending=True)
            unique_clicks_df = unique_clicks_raw.drop_duplicates(['campaign_id', 'RIID'], keep='first')
            total_unique_clicks_count = len(unique_clicks_df)
            avg_click = round(float(total_unique_clicks_count) / total_sent_count, 4)
            print('total_unique_clicks_count', total_unique_clicks_count)
            print('total_sent_df_count', total_sent_count)
            print('avg_click', avg_click)

            #if Customer.objects.filter(RIID=riid).exists():
                #print('I am herer', riid)
            Customer.objects.filter(RIID=riid).update(avg_click=avg_click)

        except Exception as e:
            print(e)


if __name__ == '__main__':
    print('data ingestion -- start -- ', datetime.now())
    start_time = datetime.now()
    obj = CustomerAttribute()


    end_time = datetime.now()
    run_time = end_time - start_time
    print('run_time : ', run_time)
    print('data ingestion -- end -- ', datetime.now())

