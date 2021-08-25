from elasticsearch import Elasticsearch
es = Elasticsearch("ELK instance 1")
es2 = Elasticsearch("ELK instance 2")
from pandas.io.json import json_normalize
from elasticsearch.helpers import bulk
import pandas as pd
import sys
import datetime

def accntlockeddata( indexname, start_time, end_time):
    # ************************************************* Collect Account Locked data from Elasticsearch***********************************

    query0 = '{"size":1000,"_source":{"includes":["@timestamp","event_id","beat.hostname","event_data.TargetUserName"]},"query":{"bool" :{"must":[{"term":{"event_id":"4740"}},{"range":{"@timestamp":{"gte":"'+start_time+'","lte":"'+end_time+'"}}}]}}}'
    data1 = es.search(index =indexname,body =(query0),scroll='1m')

    #print(data1['hits']['hits'])
    df_csv1 = pd.DataFrame(columns=['Event_ID_accountlocked','UserName_accountlocked','Account_Locked_Time'])
    df1 = json_normalize(data=data1['hits']['hits'], errors='ignore')  # Normalize into a dataframe(fst level)
    #print(df1)
    df_csv1['Account_Locked_Time'] = df1['_source.@timestamp']
    # df_csv1['Event_ID_accountlocked'] = df1['_source.event_id']
    # df_csv1['Hostname'] = df1['_source.beat.hostname']
    df_csv1['UserName_accountlocked'] = df1['_source.event_data.TargetUserName']
    df_csv1['Account_Locked_Time'] = pd.to_datetime(df_csv1['Account_Locked_Time'])
    # df_csv1.to_csv('data_Account_Locked.csv')
    e_id1 = []
    hostname1 = []
    timestamp1 = []
    username1 = []
    process_name = []
    ip_address =[]
    string = 'N/A'
    for row in range(len(df1)):
        e_id1.append(df1['_source.event_id'][row])
        hostname1.append(df1['_source.beat.hostname'][row])
        username1.append(df1['_source.event_data.TargetUserName'][row])
        timestamp1.append(df1['_source.@timestamp'][row])
        process_name.append(string)
        ip_address.append(string)




    #******************************************************Collect Times series data fro account locked***************************************************


    #df_csv1 = pd.read_csv('data_Account_Locked.csv')

    length = df_csv1['Account_Locked_Time'].count()
    n=0

    usernames = []
    for u in range(length):
        usernames.append(df_csv1['UserName_accountlocked'][u])

    hostnames = []


    t = timestamp1[0]
    e_id = []
    hostname = []
    timestamp = []
    username = []
    flag =[]
    for i in range(length):
        t = pd.to_datetime(df_csv1['Account_Locked_Time'][i],infer_datetime_format=True)
        t1 = t + datetime.timedelta(minutes=30)
        t2 = t - datetime.timedelta(minutes=30)

        query = '{"size":10000,"_source":{"includes":["@timestamp","event_id","event_data.TargetUserName","beat.hostname","event_data.ProcessName","event_data.IpAddress"]},"query": {"bool": {"must": [{"term": {"event_id": "4625"}},{"term":{"event_data.TargetUserName": "'+usernames[i]+'"}},{"range": {"@timestamp": {"gte":"' +t2.strftime("%Y-%m-%d"'T'"%H:%M:%S") + '","lte":"' + t1.strftime("%Y-%m-%d"'T'"%H:%M:%S") + '"}}}]}}}'
        #print(query)
        res = es.search(index='winlogbeat*', body=(query),scroll ='1m')
        #print(res['hits']['hits'])
        df = json_normalize(data=res['hits']['hits'],errors='ignore')  # Normalize into a data frame(fst level)
        for row in range(len(df)):
          #e_id.append(df['_source.event_id'][row])
          hostname.append(df['_source.beat.hostname'][row])
          username.append(df['_source.event_data.TargetUserName'][row])
          timestamp.append(df['_source.@timestamp'][row])

          e_id1.append(df['_source.event_id'][row])
          hostname1.append(df['_source.beat.hostname'][row])
          username1.append(df['_source.event_data.TargetUserName'][row])
          timestamp1.append(df['_source.@timestamp'][row])
          process_name.append(df['_source.event_data.ProcessName'][row])
          ip_address.append(df['_source.event_data.IpAddress'][row])



    # *******************************************************************Login Success**************************************************************************
    #  ************************************************************************************************************************************************************

    for i in range(len(hostname)):
        t = pd.to_datetime(timestamp[i], infer_datetime_format=True)
        t1 = t + datetime.timedelta(minutes=30)
        t2 = t - datetime.timedelta(minutes=30)
        query1 = '{"size":10000,"_source":{"includes":["@timestamp","event_id","event_data.TargetUserName","beat.hostname","event_data.ProcessName","event_data.IpAddress"]},"query": {"bool": {"must": [{"term": {"event_id":"4624"}},{"term":{"event_data.TargetUserName": "' + username[i] + '"}},{"term":{"beat.hostname": "'+hostname[i]+'"}},{"range": {"@timestamp": {"gte":"' + t2.strftime("%Y-%m-%d"'T'"%H:%M:%S") + '","lte":"' + t1.strftime("%Y-%m-%d"'T'"%H:%M:%S") + '"}}}]}}}'
        res = es.search(index='winlogbeat*', body=(query1),scroll = '1m')
        # print(res['hits']['hits'])
        df = json_normalize(data=res['hits']['hits'], errors='ignore')  # Normalize into a data frame(fst level)
        for row in range(len(df)):
            e_id1.append(df['_source.event_id'][row])
            hostname1.append(df['_source.beat.hostname'][row])
            username1.append(df['_source.event_data.TargetUserName'][row])
            timestamp1.append(df['_source.@timestamp'][row])
            process_name.append(df['_source.event_data.ProcessName'][row])
            ip_address.append(df['_source.event_data.IpAddress'][row])




    #********************************************************************************Password Change******************************************************************



    for i in range(len(hostname)):
        t = pd.to_datetime(timestamp[i], infer_datetime_format=True)
        t1 = t + datetime.timedelta(minutes=30)
        t2 = t - datetime.timedelta(minutes=30)
        query2 = '{"size":1000,"_source":{"includes":["@timestamp","event_id","event_data.TargetUserName","beat.hostname","event_data.ProcessName","event_data.IpAddress"]},"query": {"bool": {"must": [{"term": {"event_id":"4724"}},{"term":{"event_data.TargetUserName": "' + username[i] + '"}},{"term":{"beat.hostname": "'+hostname[i]+'"}},{"range": {"@timestamp": {"gte":"' + t2.strftime("%Y-%m-%d"'T'"%H:%M:%S") + '","lte":"' + t1.strftime("%Y-%m-%d"'T'"%H:%M:%S") + '"}}}]}}}'
        res = es.search(index='winlogbeat*', body=(query2),scroll ='1m')
        # print(res['hits']['hits'])
        df = json_normalize(data=res['hits']['hits'], errors='ignore')  # Normalize into a data frame(fst level)
        for row in range(len(df)):
            e_id1.append(df['_source.event_id'][row])
            hostname1.append(df['_source.beat.hostname'][row])
            username1.append(df['_source.event_data.TargetUserName'][row])
            timestamp1.append(df['_source.@timestamp'][row])
            process_name.append(string)
            ip_address.append(string)


    Training_data = pd.DataFrame(columns=['Event_ID', 'UserName', 'Hostname', 'TimeStamp','Process_Name','Ip_Address'])
    for r in range(len(e_id1)):
        if (e_id1[r] == 4625):
            Training_data = Training_data.append({'Event_ID': e_id1[r], 'UserName': username1[r], 'Hostname': hostname1[r], 'TimeStamp': timestamp1[r],'Process_Name': process_name[r],'Ip_Address': ip_address[r],'Accounts_Locked': 0,'Login_Failure':1,'Password_Changed':0,'Login_Success':0}, ignore_index=True)
        if (e_id1[r] == 4724):
            Training_data = Training_data.append({'Event_ID': e_id1[r], 'UserName': username1[r], 'Hostname': hostname1[r], 'TimeStamp': timestamp1[r],'Process_Name': process_name[r],'Ip_Address': ip_address[r],'Accounts_Locked': 0,'Login_Failure':0,'Password_Changed':1,'Login_Success':0}, ignore_index=True)
        if (e_id1[r] == 4624):
            Training_data = Training_data.append({'Event_ID': e_id1[r], 'UserName': username1[r], 'Hostname': hostname1[r], 'TimeStamp': timestamp1[r],'Process_Name': process_name[r],'Ip_Address': ip_address[r],'Accounts_Locked': 0,'Login_Failure':0,'Password_Changed':0,'Login_Success':1}, ignore_index=True)
        if (e_id1[r] == 4740):
            Training_data = Training_data.append({'Event_ID': e_id1[r], 'UserName': username1[r], 'Hostname': hostname1[r], 'TimeStamp': timestamp1[r],'Process_Name': process_name[r],'Ip_Address': ip_address[r],'Accounts_Locked': 1,'Login_Failure':0,'Password_Changed':0,'Login_Success':0},ignore_index=True)

    #print(Training_data)
    training_data_doc = Training_data.to_dict(orient='records')
    bulk(es2, training_data_doc, index='account_locked_correlation_analytics', doc_type='document',raise_on_error=True)
    Training_data.to_csv('Training_data.csv')


    return(0)

def main():
    # function call with system arguments
    accntlockeddata(sys.argv[1],sys.argv[2],sys.argv[3])

# invoking the main
if __name__ == "__main__":
     main()