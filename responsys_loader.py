# Django specific settings
import os
import sys
import shutil
import pandas as pd
import glob

import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()

from datetime import datetime
from dateutil import parser
from django.core.exceptions import ObjectDoesNotExist
from django.db import IntegrityError
from pandas.io.common import EmptyDataError

from activity.models import (List, Campaign, Customer, Activity, ResponsysFile)

from sqlalchemy import create_engine


def reconcile_list():
    lmap = {'heartwood': 2242,
            'insiders': 2202,
            'barclays': 2262,
            'clubhouse': 2222,
            'franklin': 2282,
            'afternoon delight wine': 2302,
            'taste': 46102,
            'martha': 95662}

    for obj in List.objects.all():
        for k, v in lmap.items():
            if k in obj.name.lower():
                obj.rlid = v
                obj.save()


class EventFile:
    def __init__(self, path, name):
        """
        convert even files to dataframe

        """
        self.fname = name
        self.full_path = os.path.join(path, name)
        self.event_type = self.get_event_type_from_name()

        if self.is_processed():
            self.df = pd.DataFrame()  # TODO: deal with processed and empty file
            return None

        try:
            self.df = self.read_data()
            if self.event_type in ['optin', 'optout']:
                # drop row if campaign_id is null
                self.df = self.df.dropna(subset=['campaign_id'])
                self.df["campaign_id"] = self.df["campaign_id"].astype(int)


        except EmptyDataError:
            self.df = pd.DataFrame()
            return None

    def read_data(self):
        fields = {'CAMPAIGN_ID': 'campaign_id',
                  'EMAIL_FORMAT': 'email_format',
                  'EVENT_CAPTURED_DT': 'timestamp',
                  'EVENT_TYPE_ID': 'action',
                  'LIST_ID': 'rlid',
                  'CUSTOMER_ID': 'customer_id',
                  'RIID': 'RIID'}

        date_fields = ['EVENT_CAPTURED_DT']
        if self.event_type == 'complaint':
            date_fields.append('COMPLAINT_DT')
            fields['REASON'] = 'reason'
            fields['COMPLAINT_DT'] = 'timestamp_complaint'
            fields['EMAIL'] = 'email'
        elif self.event_type in ['click', 'convert']:
            fields['OFFER_URL'] = 'url'
        elif self.event_type in ['optin', 'optout']:
            fields['REASON'] = 'reason'
            fields['SOURCE'] = 'source'
            fields['EMAIL'] = 'email'
        elif self.event_type in ['sent', 'bounce', 'skipped']:
            fields['EMAIL'] = 'email'

        cols = list(fields.keys())

        return pd.read_csv(self.full_path, compression='infer',
                           error_bad_lines=False, warn_bad_lines=True,
                           parse_dates=date_fields)[cols].rename(columns=fields)

    def get_event_type_from_name(self):
        """
        get event type from file name
        :return:
        """
        if 'OPT_IN' in self.fname:
            return 'optin'
        if 'OPT_OUT' in self.fname:
            return 'optout'

        return self.fname.split('_')[1].lower()

    def is_processed(self):
        """
        check the redshift db to see if the file has been processed
        :return:
        """
        qs = ResponsysFile.objects.filter(name=self.fname)
        if qs:
            return qs[0].is_processed
        else:
            return False

    def _preprocess(self):
        """
        load campaign and list data into memory to avoid repeated db query
        :return: lmap, cmap
        """
        lmap = {}
        for o in List.objects.filter(rlid__isnull=False):
            lmap[o.rlid] = o

        cmap = {}
        tmp = self.df[['campaign_id', 'rlid']].drop_duplicates()
        tmp = tmp.loc[(tmp.campaign_id != 0)]

        ary = tmp.to_dict(orient='records')

        for d in ary:
            d['src'] = 'r'  # from responsys
            rlid = d.pop('rlid')
            print('rlid :', rlid)
            elist = List.objects.get(rlid=rlid)
            d['list'] = elist
            o, _ = Campaign.objects.get_or_create(**d)
            cmap[(d['campaign_id'], rlid)] = o

        return lmap, cmap

    def process(self):
        """
        process the file, store relevant information into redshift
        :return:
        """
        lmap, cmap = self._preprocess()
        activity_data = []
        i = 0
        for _, row in self.df.iterrows():
            d = dict(row)
            if row['campaign_id'] != 0:
                i += 1
                # print('records to db  : ', x)
                d['campaign'] = cmap[(d.pop('campaign_id'), d['rlid'])]
                d['list'] = lmap[d.pop('rlid')]
                d['src'] = 'r'
                # d.pop('RIID')
                # TODO: add a field from_file to record which event file

                if self.event_type == 'complaint':
                    activity_data.append(Activity(**d))
                elif self.event_type in ['click', 'convert']:
                    activity_data.append(Activity(**d))
                elif self.event_type in ['optin', 'optout']:
                    activity_data.append(Activity(**d))
                elif self.event_type in ['bounce']:
                    activity_data.append(Activity(**d))
                elif self.event_type in ['sent', 'skipped']:
                    activity_data.append(Activity(**d))
                else:
                    activity_data.append(Activity(**d))
            if i >= 10000:
                print('bulk activity insert data to db', i)
                Activity.objects.bulk_create(activity_data)
                i = 0
                activity_data = []

        if len(activity_data) > 0:
            print('bulk activity insert data to db', i)
            Activity.objects.bulk_create(activity_data)

    def archive(self, directory):
        """
        archive the file to arhicve directory after it gets processed
        :param directory: location of the archive directory
        :return: None
        """
        shutil.move(self.full_path, directory)

    def mark_processed(self):
        try:
            obj = ResponsysFile.objects.get(name=self.fname)
            obj.is_processed = True
        except ObjectDoesNotExist:
            obj = ResponsysFile(name=self.fname, is_processed=True)
        obj.save()

    def full_process(self, dir):
        if self.df.size > 0:
            self.process()
        self.archive(dir)
        self.mark_processed()


class MemberFile:
    def __init__(self, path, name, rlid):
        self.fname = name
        self.full_path = os.path.join(path, name)
        self.list = List.objects.get(rlid=rlid)
        self.df = self.read_data()
        self.df = self.df.fillna('')
        self.df = self.df.sort_values(["email", "customer_id"], ascending=False)
        # self.df = self.df.drop_duplicates(['email'], keep='first')

    def read_data(self):
        fields = {'RIID_': 'RIID',
                  'EMAIL_ADDRESS_': 'email',
                  'EMAIL_PERMISSION_STATUS_': 'status',
                  'MODIFIED_DATE_': 'last_changed',
                  'CREATED_DATE_': 'timestamp_opt',
                  'CUSTOMER_ID_': 'customer_id'}
        cols = list(fields.keys())

        return pd.read_csv(self.full_path, compression='infer',
                           error_bad_lines=False, warn_bad_lines=True,
                           usecols=fields).rename(columns=fields)

    def process(self):
        i = 0
        member_list = []
        for row in self.df.iterrows():
            i += 1
            d = dict(row[1])
            d['list_id'] = self.list.list_id
            d['src'] = 'r'
            d['customer_id'] = str(d['customer_id']).rstrip('.0')
            if d['timestamp_opt'] in ['WS:MERGE', '']:
                d['timestamp_opt'] = None
            cmer = Customer(**d)
            member_list.append(cmer)
            if i >= 100000:
                print('bulk customer insert data to db', i)
                Customer.objects.bulk_create(member_list)
                member_list = []
                i = 0

        if len(member_list) > 0:
            print('bulk insert customer data to db', i)
            Customer.objects.bulk_create(member_list)

    def archive(self, directory):
        shutil.move(self.full_path, os.path.join(directory, self.fname))


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'reconcile_list':
        reconcile_list()
        sys.exit()

    start_time = datetime.now()
    print('data ingestion -- start -- ', start_time)
    raw_dir = 'activity/fixtures'
    archive_dir = 'activity/backup'
    #raw_dir = '/data/responsys/raw'
    #archive_dir = '/data/responsys/archive'

    print('delete customer data in Customer table')
    #Customer.objects.filter(src='r').delete()
    # TODO: eventually may need to sync customer data from customer matrix

    print('processing member files')
    for file in glob.glob('%s/*[Aa]ll.csv.zip' % raw_dir):
        print('processing %s' % file)
        _, fname = os.path.split(file)
        if fname == 'WIS_All.csv.zip':
            listid = '2202'
        elif fname == 'HO_All.csv.zip':
            listid = '2242'
        elif fname == 'BA_All.csv.zip':
            listid = '2262'
        elif fname == 'FMW_all.csv.zip':
            listid = '2282'
        elif fname == 'TOC_All.csv.zip':
            listid = '46102'
        elif fname == 'MW_All.csv.zip':
            listid = '95662'
        elif fname == 'ADW_all.csv.zip':
            listid = '2302'
        elif fname == 'CW_All.csv.zip':
            listid = '2222'
        obj = MemberFile(raw_dir, fname, listid)
        obj.process()
        obj.archive(archive_dir)

    print('processing event files')
    for file in glob.glob('%s/8781_*txt*' % raw_dir):
        print('processing %s' % file)
        _, fname = os.path.split(file)
        obj, _ = ResponsysFile.objects.get_or_create(name=fname)
        if obj.is_processed:
            print('%s is processed, skip' % file)
            os.remove(file)
        else:
            ef = EventFile(raw_dir, fname)
            ef.full_process(archive_dir)

    end_time = datetime.now()
    run_time = end_time - start_time
    print('data ingestion -- end -- ', datetime.now())
    print('run_time : ', run_time)
    print('\n')

