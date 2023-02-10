import pandas as pd
import numpy as np

#from datetime import datetime, timedelta
import datetime

import json
from pprint import pprint

from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

import matplotlib.pyplot as plt
#!pip install --upgrade seaborn
import seaborn as sns

import gc
#gc.collect()

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', 60)
pd.set_option('display.float_format', '{:.2f}'.format)

with open('./books_to_notify.txt') as f:
    s_keywords = f.readlines()
    f.close()
s_keywords = pd.Series(s_keywords).str.strip().str.replace(' ','')
s_keywords = s_keywords[s_keywords.str.len() > 0]

# data_iac = pd.read_json('http://pdfep.auction.co.kr/bookep_new/naverbook_brief.json', lines=True)
data_iac = pd.read_json('http://pdfep.auction.co.kr/bookep_new/naverbook_all.json', lines=True)
#data_iac = pd.read_json('/Users/kyunghokim/Downloads/naverbook_all_IAC_20220620.json', lines=True)
data_iac.head()

# data_gmkt = pd.read_json('http://pdfweb.gmarket.co.kr/bookep_new/naverbook_brief.json', lines=True)
data_gmkt = pd.read_json('http://pdfweb.gmarket.co.kr/bookep_new/naverbook_all.json', lines=True)
#data_gmkt = pd.read_json('/Users/kyunghokim/Downloads/naverbook_all_GMKT_20220620.json', lines=True)
data_gmkt.head()

search_period = 28
s_date = (datetime.datetime.now() - datetime.timedelta(days=search_period)).strftime('%Y-%m-%d')

#iac 1. 2022년 출간된 책 중
book_iac_2022 = data_iac[data_iac['publish_day'] > s_date]
book_iac_2022['author'] = book_iac_2022['author'].map(lambda x:x[0])

df_iac = pd.DataFrame()

for kwd in s_keywords:
    print(kwd)
    df_iac = pd.concat([df_iac, book_iac_2022[book_iac_2022['author'].map(lambda x:x.upper().strip().replace(' ','').find(kwd.upper().strip().replace(' ','')))>=0] ], axis=0)
    df_iac = pd.concat([df_iac, book_iac_2022[book_iac_2022['title'].map(lambda x:x.upper().strip().replace(' ','').find(kwd.upper().strip().replace(' ','')))>=0] ], axis=0)
df_iac.drop_duplicates(inplace=True)
df_iac.sort_index()


#gmkt 1. 2022년 출간된 책 중
book_gmkt_2022 = data_gmkt[data_gmkt['publish_day'] > s_date]
book_gmkt_2022['author'] = book_gmkt_2022['author'].map(lambda x:x[0])

df_gmkt = pd.DataFrame()

for kwd in s_keywords:
    df_gmkt = pd.concat([df_gmkt, book_gmkt_2022[book_gmkt_2022['author'].map(lambda x:x.upper().strip().replace(' ','').find(kwd.upper().strip().replace(' ','')))>=0] ], axis=0)
    df_gmkt = pd.concat([df_gmkt, book_gmkt_2022[book_gmkt_2022['title'].map(lambda x:x.upper().strip().replace(' ','').find(kwd.upper().strip().replace(' ','')))>=0] ], axis=0)
df_gmkt.drop_duplicates(inplace=True)
df_gmkt#.sort_index()


#output

df_output = pd.concat([df_iac, df_gmkt], axis=0)
df_output.drop_duplicates(subset=['isbn'], inplace=True)
df_output.drop_duplicates(subset=['title'], inplace=True)

import requests
max_retry = 30

attempts = 0
while attempts < max_retry:
    try:
        with open("./mySlackAPIKey.txt") as f:
            lines = f.readlines()
            myToken = lines[0].strip()
            channel_id = lines[1].strip()


        def post_message(token, channel, text):
            response = requests.post("https://slack.com/api/chat.postMessage",
                headers={"Authorization": "Bearer "+token},
                data={"channel": channel,"text": text}
            )
            print(response)

        slack_message = str(df_output)

        post_message(myToken,"#cointrading", slack_message)      
        
        break
    except:
        print("seems like the proxy is going through some issues... let's retry : ", attempts)
        attempts = attempts + 1

del data_iac, data_gmkt, book_iac_2022, book_gmkt_2022