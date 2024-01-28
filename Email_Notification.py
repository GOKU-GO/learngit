import pandas as pd
import psycopg2
import csv
import sys
import os 
from datetime import date
from datetime import timedelta
import io
import warnings
warnings.filterwarnings("ignore")
import boto3
import s3fs
import json
import datetime
from botocore.exceptions import ClientError
from io import StringIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import emails


def Pro_create_presigned_url(expiration=604800):

    # connect to s3
    s3 = boto3.client('s3',aws_access_key_id='',aws_secret_access_key = '',region_name = '')
   
    # define filename
    bucket_name = "caratlane-intent"
    today_date = str(datetime.datetime.today())
    curr_date = datetime.datetime.today().strftime('%Y-%m-%d')
    object_name_new = "ML_Prospect_Automation/"+curr_date+"-Prospect_Pro_new.csv"
    object_name_repeat = "ML_Prospect_Automation/"+curr_date+"-Prospect_Pro_repeat.csv"

    # generate new leads url
    try:
        response1 = s3.generate_presigned_url('get_object',
                                             Params={'Bucket': bucket_name,
                                                     'Key': object_name_new},
                                             ExpiresIn=expiration)
        #print(type(response))
        #print(response)
    except:
        pass

    # generate new leads url
    try:
        response2 = s3.generate_presigned_url('get_object',
                                             Params={'Bucket': bucket_name,
                                                     'Key': object_name_repeat},
                                             ExpiresIn=expiration)
        #print(type(response))
        #print(response)
    except:
        pass

    return response1,response2




def Pro_smtp(link_new,link_repeat):                     #def smtp(link_new,link_repeat):



    host = ""
    smtp_username = ""
    smtp_password = ""
    port = 587
    sender_email = "information@caratlane.com"
    SUBJECT = "Prospect Pro Leads"
    RECIPIENT = ["deepaksahani.r@caratlane.com","kumaran.v@caratlane.com","sundar.g@caratlane.com","sandhyalakshmi.k@caratlane.com","suryaramesh.r@caratlane.com",
    "varun.r@caratlane.com","gladston.s@caratlane.com","suraj.g@caratlane.com","tushar.m@caratlane.com","ashish.a@caratlane.com","akshay.j@caratlane.com","gokulavasan.r@caratlane.com"]

    # RECIPIENT = ["deepaksahani.r@caratlane.com"]

    
    BODY_HTML = "Hi Team, Please find the below S3 URL to access the Prospect Pro new leads"  +"<br>"+ link_new + "<br>" +" Hi Team, Please find the below S3 URL to access the Prospect Pro repeat leads" + "<br>" +link_repeat
    # for url in url_list:
    #     BODY_HTML = BODY_HTML + "<p>" + url + "</p>"
    # Prepare the email
    message = emails.html(
        html= BODY_HTML,
        subject= SUBJECT,
        mail_from=sender_email,
    )

    # Send the email
    r = message.send(
        to= RECIPIENT, 
        smtp={
            "host": host, 
            "port": port, 
            "timeout": 5,
            "user": smtp_username,
            "password": smtp_password,
            "tls": True,
        },
    )

    # Check if the email was properly sent
    assert r.status_code == 250



