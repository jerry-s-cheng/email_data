import os, os.path
import pandas as pd
import shutil
import zipfile
import datetime
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
django.setup()

from activity.models import (List, Campaign, Customer,
                             Activity, ResponsysFile)
from datetime import datetime

mailfolder_zip = "/data/raw_data/source/responsys/download"
unzipfilefolder = "/data/application/python/dmp/sync/Email/activity/fixtures"

filesizelimit = 20000


def dataset_partition(fullpathfilename, filename):

    qiege = ((os.path.getsize(fullpathfilename) / 1024) / filesizelimit) + 1
    if qiege > 1:
        ysfile = open(fullpathfilename, 'r')
        print ('partitioning on :', fullpathfilename)
        readfiles = ysfile.readlines()
        leng = len(readfiles)
        leng = leng / qiege
        i = 0
        x = 1
        qg = str(x) + '_' + filename
        qiegefile = open(unzipfilefolder + '/' + qg, 'w')
        print ('file name :', qg)
        for line in readfiles:
            try:
                print
                line[0:-1]
                if i > leng:
                    x += 1
                    i = 0
                    qg = str(x) + '_' + filename
                    print ('file name :', qg)
                    qiegefile.close()
                    qiegefile = open(unzipfilefolder + '/' + qg, 'w')
                    if 'SENT' in filename:
                        qiegefile.writelines(
                            '"EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID"'
                            ',"EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID"'
                            ',"EMAIL","EMAIL_ISP","EMAIL_FORMAT","OFFER_SIGNATURE_ID"'
                            ',"DYNAMIC_CONTENT_SIGNATURE_ID","MESSAGE_SIZE","SEGMENT_INFO"'
                            ',"CONTACT_INFO"')
                        qiegefile.writelines('\n')
                    elif 'OPEN' in filename:
                        qiegefile.writelines(
                            '"EVENT_TYPE_ID","ACCOUNT_ID","LIST_ID","RIID","CUSTOMER_ID"'
                            ',"EVENT_CAPTURED_DT","EVENT_STORED_DT","CAMPAIGN_ID","LAUNCH_ID"'
                            ',"EMAIL_FORMAT"')
                        qiegefile.writelines('\n')

                i += 1
                qiegefile.writelines(line)
            except Exception as e:
                print (e)
        qiegefile.close()
        os.remove(fullpathfilename)


def unzip_file(zipfilename, unziptodir, onlyfilename):
    try:
        if not os.path.exists(unziptodir): os.mkdir(unziptodir, 0o777)
        print(onlyfilename)
        file_name = str(onlyfilename).rstrip('.txt.zip')
        if ResponsysFile.objects.filter(name__contains=file_name).exists():
            print(onlyfilename, 'found')
        else:
            print(onlyfilename, 'not found')
            zfobj = zipfile.ZipFile(zipfilename)
            for name in zfobj.namelist():
                name = name.replace('\\', '/')

                if name.endswith('/'):
                    os.mkdir(os.path.join(unziptodir, name))
                else:
                    ext_filename = os.path.join(unziptodir, name)
                    ext_dir = os.path.dirname(ext_filename)
                    if not os.path.exists(ext_dir): os.mkdir(ext_dir, 0o777)
                    outfile = open(ext_filename, 'wb')
                    outfile.write(zfobj.read(name))
                    outfile.close()


    except Exception as e:
        print (e)


def zip_dir(dirname, zipfilename):
    filelist = []
    if os.path.isfile(dirname):
        filelist.append(dirname)
    else:
        for root, dirs, files in os.walk(dirname):
            for name in files:
                filelist.append(os.path.join(root, name))

    zf = zipfile.ZipFile(zipfilename, "w", zipfile.zlib.DEFLATED)
    for tar in filelist:
        arcname = tar[len(dirname):]
        zf.write(tar, arcname)
    zf.close()


if __name__ == '__main__':

    print('data ingestion -- start -- ', datetime.now())
    start_time = datetime.now()

    for root, dirs, files in os.walk(mailfolder_zip):
        convert_zipfilefolder = root.replace('\\', '/')
        for f in files:
            zipfilefolder = convert_zipfilefolder + '/' + f
            unzip_file(zipfilefolder, unzipfilefolder, f)


    for root, dirs, files in os.walk(unzipfilefolder):
        for f in files:
            if 'SENT' in f or 'OPEN' in f:
                dataset_partition(os.path.join(root, f), f)

    end_time = datetime.now()
    run_time = end_time - start_time
    print('run_time : ', run_time)
    print('data ingestion -- end -- ', datetime.now())


