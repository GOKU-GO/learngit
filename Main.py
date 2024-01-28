import pandas as pd
import numpy as np
import sqlite3
import psycopg2
import csv
import sys
from datetime import date

from datetime import timedelta
import os
import io
import warnings
warnings.filterwarnings("ignore")
import boto3
import json
import datetime
from botocore.exceptions import ClientError
from io import StringIO


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# Importing functions from different files

from SaveToS3 import Ppro_write_to_aws
from Email_Notification import Pro_create_presigned_url, Pro_smtp
from Prospect_Pro import Repeat_Prospect_Pro, New_Prospect_Pro



# Prospect Pro Code

print(1)

Repeat_pro = Repeat_Prospect_Pro()

print(Repeat_pro.head(5))

New_pro = New_Prospect_Pro()

print(New_pro.head(5))

# Saving the Prospect Pro Leads in S3

Ppro_write_to_aws(Repeat_pro,New_pro)

# Getting the link to share the leads over the mail


Pro_new_link, Pro_repeat_link = Pro_create_presigned_url(expiration=604800)

Pro_smtp(Pro_new_link,Pro_repeat_link)

print("Pro Leads Email Send")