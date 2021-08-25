from elasticsearch import Elasticsearch
es = Elasticsearch("ELK instance 1")
es2 = Elasticsearch("ELK instance 2")
from pandas.io.json import json_normalize
from elasticsearch.helpers import bulk
import pandas as pd
import sys
import datetime


# ************************************************************Collect Login Failure data from elasticsearch***********************************************-------------------------------------------------

#
#
# data = es.search(index ='winlogbeat-6.6.2-2019.07.25',body =({"size":1000,"_source":{"includes":["@timestamp","event_id","event_data.TargetUserName"]},"query":{"bool" :{"must":[{"term":{"event_id":"4625"}}]}}}),scroll='1m')
#
# #print(data)
# df_csv = pd.DataFrame(columns=['Event_ID_loginfail','UserName_loginfail','Login_Failure_Time'])
# df = json_normalize(data=data['hits']['hits'], errors='ignore')  # Normalize into a dataframe(fst level)
# df_csv['Login_Failure_Time'] = df['_source.@timestamp']
# df_csv['Event_ID_loginfail'] = df['_source.event_id']
# df_csv['UserName_loginfail'] = df['_source.event_data.TargetUserName']
# df_csv['Login_Failure_Time'] = pd.to_datetime(df_csv['Login_Failure_Time'])
# #print(df_csv)
# #df_csv.info()
# df_csv.to_csv('data_Login_Failure.csv')
#
#
#
#
#
# # ****************************************************************Create Login Failure Time series*******************************************************--------------------------------------------------------------------------------------
#
#
# #df_csv = pd.read_csv('data_Login_Failure.csv')
#
# df_csv['Login_Failure_Time'] = pd.to_datetime(df_csv['Login_Failure_Time'],infer_datetime_format=True)
# df_csv['Login_Failure_Time'].sort_values
# length = df_csv['Login_Failure_Time'].count()
# n=0
#
#
# num1 = []
# t = df_csv['Login_Failure_Time'][0]
# for i in range(length):
#     num = []
#     t = df_csv['Login_Failure_Time'][i]
#     t1 = t
#     T1 = []
#     T2 = []
#     num.append(t)
#     for n in range(10):
#         t2 = t1 - datetime.timedelta(minutes=5)
#         T1.append(t1)
#         T2.append(t2)
#         t1 = t1-datetime.timedelta(minutes=5)
#
#     for x in range(len(T1)):
#         query = '{"query": {"bool": {"must": [{"term": {"event_id": "4625"}},{"range": {"@timestamp": {"gte":"' + T2[x].strftime(
#             "%Y-%m-%d"'T'"%H:%M:%S") + '","lte":"' + T1[x].strftime("%Y-%m-%d"'T'"%H:%M:%S") + '"}}}]}},"size":0,"aggs":{"Login_Failures":{"terms":{"field":"event_id"}}}}'
#         res = es.search(index='winlogbeat-6.6.2-2019.07.25', body=(query))
#         df1 = json_normalize(data=res['aggregations']['Login_Failures']['buckets'],errors='ignore')  # Normalize into a dataframe(fst level)
#         #print(df1)
#
#         c = df1.count()
#         for z in range(len(df1)):
#             num.append(df1['doc_count'][z])
#
#     #print(num)
#     num1.append(num)
#
# #print(num1)
#
#
# time_series = pd.DataFrame(num1,columns = ['Login_Failure_Time','Time0','Time1','Time2','Time3','Time4','Time5','Time6','Time7','Time8','Time9'])
# #print(time_series)
#
# Training_data = pd.merge(df_csv,time_series,on='Login_Failure_Time')
# Training_data.to_csv('Training_data_Login_Failure.csv')
#
# time_series.to_csv('time_series_Login_Failure.csv')

# **********************************************
# **********************************************************
# *************************************************************************
# *************************************************************************************



# ************************************************* Collect Account Locked data from Elasticsearch***********************************




data1 = es.search(index ='winlogbeat-6.6.2-2019.08.05',body =({"size":1000,"_source":{"includes":["@timestamp","event_id","event_data.TargetUserName"]},"query":{"bool" :{"must":[{"term":{"event_id":"4740"}}]}}}),scroll='1m')

#print(data)
df_csv1 = pd.DataFrame(columns=['Event_ID_accountlocked','UserName_accountlocked','Account_Locked_Time'])
df1 = json_normalize(data=data1['hits']['hits'], errors='ignore')  # Normalize into a dataframe(fst level)
df_csv1['Account_Locked_Time'] = df1['_source.@timestamp']
df_csv1['Event_ID_accountlocked'] = df1['_source.event_id']
df_csv1['UserName_accountlocked'] = df1['_source.event_data.TargetUserName']
df_csv1['Account_Locked_Time'] = pd.to_datetime(df_csv1['Account_Locked_Time'])
df_csv1.to_csv('data_Account_Locked.csv')






# #**************************************************************Collect Times series data fro account locked*********************************************


#df_csv1 = pd.read_csv('data_Account_Locked.csv')

length = df_csv1['Account_Locked_Time'].count()
n=0

usernames = []
for u in range(length):
    usernames.append(df_csv1['UserName_accountlocked'][u])
num1 = []
t = df_csv1['Account_Locked_Time'][0]
for i in range(length):
    num = []
    t = pd.to_datetime(df_csv1['Account_Locked_Time'][i],infer_datetime_format=True)
    t1 = t
    T1 = []
    T2 = []
    num.append(t)
    for n in range(10):
        t2 = t1 - datetime.timedelta(minutes=1)
        T1.append(t1)
        T2.append(t2)
        t1 = t1-datetime.timedelta(minutes=1)

    for x in range(len(T1)):
        query = '{"query": {"bool": {"must": [{"term": {"event_id": "4625"}},{"term":{"event_data.TargetUserName": "'+usernames[i]+'"}},{"range": {"@timestamp": {"gte":"' + T2[x].strftime("%Y-%m-%d"'T'"%H:%M:%S") + '","lt":"' + T1[x].strftime("%Y-%m-%d"'T'"%H:%M:%S") + '"}}}]}},"size":0,"aggs":{"Login_Failures":{"terms":{"field":"event_id"}}}}'

        res = es.search(index='winlogbeat-6.6.2-2019.08.05', body=(query))
        df1 = json_normalize(data=res['aggregations']['Login_Failures']['buckets'],errors='ignore')  # Normalize into a dataframe(fst level)
        #print(df1)

        c = df1.count()
        for z in range(len(df1)):
            num.append(df1['doc_count'][z])

    lsh = 11 - len(num)
    for ln in range(lsh):
        num.append(0)
    #print(num)
    num1.append(num)

#print(num1)


time_series = pd.DataFrame(num1,columns = ['Account_Locked_Time','Login_Time_m5','Login_Time_m10','Login_Time_m15','Login_Time_m20','Login_Time_m25','Login_Time_m30','Login_Time_m35','Login_Time_m40','Login_Time_m45','Login_Time_m50'])
#print(time_series)

Training_data = pd.merge(df_csv1,time_series,on='Account_Locked_Time')
Training_data.to_csv('Training_data_Account_Locked.csv')

time_series.to_csv('time_series_Account_Locked.csv')





