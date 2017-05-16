# Django specific settings
import os
import shutil
import pandas as pd

import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()

from datetime import datetime
from dateutil import parser
from django.core.exceptions import ObjectDoesNotExist
from django.db import IntegrityError

from activity.models import (List, Campaign, Customer,
                             Activity, ResponsysFile)


class MemberFile:
    def __init__(self, path, name, rlid):
        self.fname = name
        self.full_path = os.path.join(path, name)
        self.list = List.objects.get(rlid=rlid)
        self.df = self.read_data()
        self.df = self.df.fillna('')
        self.df = self.df.sort_values(["email", "customer_id"], ascending=False)
        self.df = self.df.drop_duplicates(['email'], keep='first')


    def read_data(self):
        fields = {'RIID_': 'RIID',
                  'EMAIL_ADDRESS_': 'email',
                  'EMAIL_PERMISSION_STATUS_': 'status',
                  'MODIFIED_DATE_': 'modified_date',
                  'CREATED_DATE_': 'created_date',
                  'CUSTOMER_ID_': 'customer_id'}
        cols = list(fields.keys())

        return pd.read_csv(self.full_path, compression='infer',
                           usecols=fields).rename(columns=fields)

    def process(self):
        i = 0
        member_list = []
        for row in self.df.iterrows():
            i = i + 1
            if i < 130001:
                d = dict(row[1])
                d['list'] = self.list
                customer_id = str(d['customer_id']).rstrip('.0')
                member_list.append(Customer(RIID=d['RIID'], email=d['email'], status=d['status'],
                                            list_id=d['list'].list_id))

            else:
                print('bulk customer insert data to db', i)
                Customer.objects.bulk_create(member_list)
                member_list = []
                i = 0

        Customer.objects.bulk_create(member_list)


if __name__ == '__main__':
    print('data ingestion -- start -- ', datetime.now())
    start_time = datetime.now()
    s = 'activity/fixtures'
    b = 'activity/backup'

    print('delete customer data in Customer table' )
    Customer.objects.all().delete()
    # Customer.objects.filter(list='ab9dac8057').delete()
    for root, dirs, files in os.walk(s):
        for f in files:
            if os.path.getsize(os.path.join(root, f)) > 0:
                if f == 'WIS_All.csv' or f == 'HO_All.csv' or f == 'Barclays_all.csv':
                # process Member File
                    print('reload file name : ', f)
                    if f == 'WIS_All.csv':
                        listid = '2202'
                    elif f == 'HO_All.csv':
                        listid = '2242'
                    elif f == 'Barclays_all.csv':
                        listid = '2262'

                    obj = MemberFile(s, f, listid)
                    obj.process()

    end_time = datetime.now()
    run_time = end_time - start_time
    print('run_time : ', run_time)
    print('data ingestion -- end -- ', datetime.now())
