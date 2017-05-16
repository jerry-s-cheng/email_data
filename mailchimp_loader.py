# Django specific settings
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")

# Ensure settings are read
from django.core.wsgi import get_wsgi_application

application = get_wsgi_application()

from datetime import datetime
from dateutil import parser
from django.core.exceptions import ObjectDoesNotExist
from django.db import IntegrityError
from pymongo import MongoClient
from activity.models import List, Campaign, Customer, Activity
from config import ACTION_MAP

client = MongoClient('mongodb://localhost:xxxxx/')
db = client.chimp_test


def save(obj, warn_dup=False):
    try:
        obj.save()
    except IntegrityError:
        if warn_dup:
            print('exists')  # TODO: log and process dupe
        else:
            pass


def sync_list():
    for doc in db.lists.find():
        obj = List()
        obj.list_id = doc.get('id')
        obj.name = doc.get('name')
        stats = doc.get('stats')
        obj.member_count = stats.get('member_count')
        obj.unsubscribe_count = stats.get('unsubscribe_count')
        obj.src = 'm'
        save(obj)


def sync_members():
    i = 0
    for doc in db.members.find(no_cursor_timeout=True):
        i = i + 1
        elist = List.objects.get(list_id=doc.get('list_id'))
        email = doc.get('email_address')
        try:
            obj = Customer.objects.get(list=elist, email=email)
        except ObjectDoesNotExist:
            obj = Customer(list=elist, email=email)
        obj.timestamp_opt = doc.get('timestamp_opt')
        obj.last_changed = doc.get('last_changed')
        # use responsys subscription status code
        obj.status = 'I' if doc.get('status') == 'subscribed' else 'O'
        obj.src = 'm'
        try:
            save(obj)
            print('member to db - ', i)
        except Exception as e:
            print(e)

def sync_campaigns():
    i = 0
    for doc in db.campaigns.find(no_cursor_timeout=True):
        i = i + 1
        campaign_id = doc.get('id')
        lid = doc.get('recipients').get('list_id')
        elist = List.objects.get(list_id=lid)
        try:
            obj = Campaign.objects.get(list=elist, campaign_id=campaign_id)
        except ObjectDoesNotExist:
            obj = Campaign(list=elist, campaign_id=campaign_id)
        obj.send_time = doc.get('send_time')
        obj.emails_sent = doc.get('emails_sent')
        obj.src = 'm'
        if doc.get('report_summary'):
            report = doc.get('report_summary')
            obj.opens = report.get('opens')
            obj.unique_opens = report.get('unique_opens')
            obj.clicks = report.get('subscriber_clicks')
            obj.click_rate = report.get('click_rate')
            obj.open_rate = report.get('open_rate')
            try:
                save(obj)
                print('campaigns to db - ', i)
            except Exception as e:
                print (e)


def sync_actions():
    n = 0
    for doc in db.activities.find(no_cursor_timeout=True):
        cid = doc.get('campaign_id')
        lid = doc.get('list_id')
        email = doc.get('email_address')

        campaign = Campaign.objects.get(campaign_id=cid)
        elist = List.objects.get(list_id=lid)
        customer = Customer.objects.get(list=elist, email=email)

        # sent event
        obj = Activity(campaign=campaign, customer=customer, list=elist)
        obj.action = ACTION_MAP['sent']
        obj.timestamp = campaign.send_time
        save(obj)

        # other event
        activities = doc.get('activity')
        for elem in activities:
            timestamp = elem.get('timestamp')
            if type(timestamp) == str:
                # 2015-10-13T13:31:51+00:00
                timestamp = parser.parse(timestamp)
            url = elem.get('url', None)
            ip = elem.get('ip', None)
            action = elem.get('action')

            obj = Activity()
            obj.campaign = campaign
            obj.customer = customer
            obj.list = elist
            obj.action = ACTION_MAP[action]
            obj.timestamp = timestamp
            obj.ip = ip
            obj.url = url
            obj.src = 'm'

            save(obj)

        n += 1
        if n % 10000 == 0:
            print(n)


if __name__ == '__main__':
    sync_list()
    #sync_members()
    #sync_campaigns()
    #sync_actions()

