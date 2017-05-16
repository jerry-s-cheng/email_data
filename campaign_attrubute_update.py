# Django specific settings
import os
import django_redshift_backend

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
  'postgresql://xxxxx.amazonaws.com:5432/xxxxx/')



class CampaignAttribute:

    def __init__(self):
        self.db = email_data_engine
        self.read_data()
        #self.update_emails_send()
        #self.update_opens()
        #self.update_clicks()
        #self.updaet_unique_opens()
        #self.updaet_unique_clicks()
        #self.update_send_time()

        #campaign_id_list = []
        campaign_id_list = self.raw_campaign_df['campaign_id']

        for campaign_id in campaign_id_list:
            self.update_open_rate(campaign_id)
            #self.update_click_rate(campaign_id)


    def read_data(self):
        sql_activity = 'select "RIID", action, timestamp, campaign_id, list_id, customer_id ' \
                       'from email_data.activity_activity where action in (1, 2, 4, 5, 18)'
        sql_campaign = 'select distinct(campaign_id) from email_data.activity_campaign'
        self.raw_activity_df = pd.read_sql(sql_activity, self.db)
        print (self.raw_activity_df)
        self.raw_campaign_df = pd.read_sql(sql_campaign, self.db)



    def update_send_time(self):
        """
                update send time information to db

                """
        send_time_df = self.raw_activity_df.loc[(self.raw_activity_df['action'] == 1)]
        send_time_df = send_time_df.groupby(['campaign_id'], sort=False)['timestamp'].max()

        for campaign_id, timestamp in send_time_df.iteritems():
           print(campaign_id, timestamp)
           Campaign.objects.filter(campaign_id=campaign_id).update(send_time=timestamp)


    def update_emails_send(self):
        """
                update emails send information to db

                """
        emails_send_df = self.raw_activity_df[(self.raw_activity_df['action'] == 1)].groupby('campaign_id').size()
        for campaign_id, emails_sent in emails_send_df.iteritems():
            print (campaign_id, emails_sent)
            Campaign.objects.filter(campaign_id=campaign_id).update(emails_sent=emails_sent)


    def update_opens(self):
        """
                update opens information to db

                """
        opens_df = self.raw_activity_df[(self.raw_activity_df['action'] == 4)].groupby('campaign_id').size()
        for campaign_id, opens in opens_df.iteritems():
            print (campaign_id, opens)
            Campaign.objects.filter(campaign_id=campaign_id).update(opens=opens)


    def update_clicks(self):
        """
                update clicks information to db

                """
        clicks_df = self.raw_activity_df[(self.raw_activity_df['action'] == 5)].groupby('campaign_id').size()
        for campaign_id, clicks in clicks_df.iteritems():
            #print (campaign_id, clicks)
            Campaign.objects.filter(campaign_id=campaign_id).update(clicks=clicks)


    def updaet_unique_opens(self):
        """
                update unique open information to db

                """
        #unique_opens_raw = self.raw_activity_df[(self.raw_activity_df['action'] == 4) & (self.raw_activity_df['RIID'] != 'nan')]
        unique_opens_raw = self.raw_activity_df[(self.raw_activity_df['action'] == 4)]
        unique_opens_raw = unique_opens_raw.sort_values(['campaign_id', 'RIID'], ascending=True)
        unique_opens_raw = unique_opens_raw.drop_duplicates(['campaign_id','RIID'], keep='first')
        unique_opens_df = unique_opens_raw.groupby('campaign_id').size()
        for campaign_id, unique_opens in unique_opens_df.iteritems():
            print (campaign_id, unique_opens)
            Campaign.objects.filter(campaign_id=campaign_id).update(unique_opens=unique_opens)


    def updaet_unique_clicks(self):
        """
                update unique clicks information to db

                """
        #unique_clicks_raw = self.raw_activity_df[(self.raw_activity_df['action'] == 5) & (self.raw_activity_df['RIID'] != 'nan')]
        unique_clicks_raw = self.raw_activity_df[(self.raw_activity_df['action'] == 5)]
        unique_clicks_raw = unique_clicks_raw.sort_values(['campaign_id', 'RIID'], ascending=True)
        unique_clicks_raw = unique_clicks_raw.drop_duplicates(['campaign_id','RIID'], keep='first')
        unique_clicks_df = unique_clicks_raw.groupby('campaign_id').size()
        for campaign_id, unique_clicks in unique_clicks_df.iteritems():
            print (campaign_id, unique_clicks)
            Campaign.objects.filter(campaign_id=campaign_id).update(unique_clicks=unique_clicks)


    def update_open_rate(self, campaign_id):
        """
                update open rate information to db

                total_open = self.raw_activity_df[
                (self.raw_activity_df['action'] == 4) & (self.raw_activity_df["campaign_id"] == campaign_id)].size
            total_sent = self.raw_activity_df[
                (self.raw_activity_df['action'] == 1) & (self.raw_activity_df["campaign_id"] == campaign_id)].size
            total_bounced = self.raw_activity_df[
                (self.raw_activity_df['action'] == 2) & (self.raw_activity_df["campaign_id"] == campaign_id)].size
            total_complain = self.raw_activity_df[
                (self.raw_activity_df['action'] == 18) & (self.raw_activity_df["campaign_id"] == campaign_id)].size

                """
        try:
            total_open = len(self.raw_activity_df[
                (self.raw_activity_df['action'] == 4) & (self.raw_activity_df["campaign_id"] == campaign_id)])
            total_sent = len(self.raw_activity_df[
                (self.raw_activity_df['action'] == 1) & (self.raw_activity_df["campaign_id"] == campaign_id)])
            total_bounced = len(self.raw_activity_df[
                (self.raw_activity_df['action'] == 2) & (self.raw_activity_df["campaign_id"] == campaign_id)])
            total_complain = len(self.raw_activity_df[
                (self.raw_activity_df['action'] == 18) & (self.raw_activity_df["campaign_id"] == campaign_id)])

            total_count = float(total_sent - total_bounced - total_complain)
            open_rate = round(float(total_open) / total_count, 4)
            print (open_rate)
            Campaign.objects.filter(campaign_id=campaign_id).update(open_rate=open_rate)
        except Exception as e:
            print (e)


    def update_click_rate(self,campaign_id):
        """
                update click rate information to db
                total_click = self.raw_activity_df[
                (self.raw_activity_df['action'] == 5) & (self.raw_activity_df["campaign_id"] == campaign_id)].size
            total_sent = self.raw_activity_df[
                (self.raw_activity_df['action'] == 1) & (self.raw_activity_df["campaign_id"] == campaign_id)].size
            total_bounced = self.raw_activity_df[
                (self.raw_activity_df['action'] == 2) & (self.raw_activity_df["campaign_id"] == campaign_id)].size
            total_complain = self.raw_activity_df[
                (self.raw_activity_df['action'] == 18) & (self.raw_activity_df["campaign_id"] == campaign_id)].size

                """
        try:
            total_click = len(self.raw_activity_df[
                (self.raw_activity_df['action'] == 5) & (self.raw_activity_df["campaign_id"] == campaign_id)])
            total_sent = len(self.raw_activity_df[
                (self.raw_activity_df['action'] == 1) & (self.raw_activity_df["campaign_id"] == campaign_id)])
            total_bounced = len(self.raw_activity_df[
                (self.raw_activity_df['action'] == 2) & (self.raw_activity_df["campaign_id"] == campaign_id)])
            total_complain = len(self.raw_activity_df[
                (self.raw_activity_df['action'] == 18) & (self.raw_activity_df["campaign_id"] == campaign_id)])

            total_count = float(total_sent - total_bounced - total_complain)
            click_rate = round(float(total_click) / total_count, 4)
            Campaign.objects.filter(campaign_id=campaign_id).update(click_rate=click_rate)

        except Exception as e:
            print (e)


if __name__ == '__main__':
    print('data ingestion -- start -- ', datetime.now())
    start_time = datetime.now()
    obj = CampaignAttribute()


    end_time = datetime.now()
    run_time = end_time - start_time
    print('run_time : ', run_time)
    print('data ingestion -- end -- ', datetime.now())

