import pandas as pd
import numpy as np
import sqlite3
import psycopg2
import csv
import sys
from datetime import date
from datetime import timedelta
import boto3
import os
import io
import warnings
warnings.filterwarnings("ignore")
import json
import datetime
from botocore.exceptions import ClientError

from io import StringIO


def Ppro_write_to_aws(Rpro,Npro):
#save to s3sadfsafsadfdsaf
#access key
    aws_access_key_id = ""
    aws_secret_access_key = ""
    region_name = ""

#connect to s3

    s3_client = boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key = aws_secret_access_key,region_name = region_name)

    
    curr_date = datetime.datetime.today().strftime('%Y-%m-%d')
    filename_n = "ML_Prospect_Automation/"+curr_date+"-Prospect_Pro_new.csv"
    filename_r = "ML_Prospect_Automation/"+curr_date+"-Prospect_Pro_repeat.csv"
    
    
    # s3 = boto3.client("s3",\region_name=region_name,\
    #               aws_access_key_id=aws_access_key_id,\
    #               aws_secret_access_key=aws_secret_access_key)
    
    csv_buf = StringIO()
    Npro.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3_client.put_object(Bucket='caratlane-intent', Body=csv_buf.getvalue(), Key=filename_n)
    # print('n')
    
    
    csv_buf = StringIO()	
    Rpro.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    s3_client.put_object(Bucket='caratlane-intent', Body=csv_buf.getvalue(), Key=filename_r)
    # print('r')
    
