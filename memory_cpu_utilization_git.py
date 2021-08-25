from elasticsearch import Elasticsearch
es = Elasticsearch("ELK instance 1 ")
es2 = Elasticsearch("ELK instance 2")
from pandas.io.json import json_normalize
from elasticsearch.helpers import bulk
import pandas as pd
import sys
import datetime

def memory_utilization(start_time,end_time, indexname,percentfilter, timedelta_minus, timedelta_plus,new_indexname):
    # Empty Data frame
    df_memory_utilization = pd.DataFrame(columns=["Timestamp", "Hostname", "Total_Memory_Utilization","Total_CPU_Utilization",
                                                  "ProcessName", "Process_Memory_Utilization","CPU_utilization_per_process"])

#  ***********************************************************Total_Memory Utilization > 90%*****************************************************************************
    # Query to retrieve system memory usage greater than a given percentage
    query = '{"size":100,"_source":{"includes":["@timestamp","host.name","system.memory.used.pct"]},"query":{"bool": {"must":[{"range" : {"system.memory.used.pct" : {"gte":"'+percentfilter+'"}}},{"range" : {"@timestamp":{"lte":"'+end_time+'","gte":"'+start_time+'"}}},{"exists": {"field":"system.memory.used.pct"}}]}}}'
    # Python request to elastic search
    data = es.search(index= indexname,body = (query),scroll= '1m')
    # elastic search response to data frame
    df = json_normalize(data = data['hits']['hits'],errors = 'ignore')
    count = df['_source.@timestamp'].count()
    #.strftime("%Y-%m-%d"'T'"%H:%M:%S")
    # for loop to append data frame
    for i in range(count):
        df_memory_utilization = df_memory_utilization.append(
            {"Timestamp": df['_source.@timestamp'][i],"Hostname": df['_source.host.name'][i],"Total_Memory_Utilization":df['_source.system.memory.used.pct'][i]}, ignore_index=True)

#  ************************************************************Total_CPU_Utilization*****************************************************************************
    # for loop to create timestamp window by adding and subtracting a given time delta from
    # the time memory utilization is greater than the requested percentage.
    for i in range(count):
        t1 = pd.to_datetime(df['_source.@timestamp'][i])

        tdlm = t1 - datetime.timedelta(minutes=int(timedelta_minus))
        tdlp = t1 + datetime.timedelta(minutes=int(timedelta_plus))
        hostname = df['_source.host.name'][i]
        # query to retrieve total cpu utilization for the requested time window
        query0 = '{"_source":{"includes":["@timestamp","host.name","system.cpu.total.pct"]},"sort": [{"system.cpu.total.pct": {"order": "desc"}}],"size": 3,"query":{"bool": {"must":[{"term":{"host.name":"'+hostname+'" }},{"range" : {"@timestamp":{"lte":"' + tdlp.strftime("%Y-%m-%d"'T'"%H:%M:%S") + '","gte":"' + tdlm.strftime("%Y-%m-%d"'T'"%H:%M:%S") + '"}}},{"exists": {"field":"system.cpu.total.pct"}}]}}}'
        # query response to data frame
        data0 = es.search(index=indexname, body=(query0), scroll='1m')
        df0 = json_normalize(data=data0['hits']['hits'], errors='ignore')
        length = df0['_source.@timestamp'].count()
        # .strftime("%Y-%m-%d"'T'"%H:%M:%S")
        for n in range(length):
            df_memory_utilization = df_memory_utilization.append(
                {"Timestamp": df0['_source.@timestamp'][n], "Hostname": df0['_source.host.name'][n],
                 "Total_CPU_Utilization": df0['_source.system.cpu.total.pct'][n]}, ignore_index=True)

#   *************************************************************Process_Memory_Utilization***********************************************************
    # for loop to retrieve process information for the requested time window
    for i in range(count):
        t1 = pd.to_datetime(df['_source.@timestamp'][i])

        tdlm = t1- datetime.timedelta(minutes= int(timedelta_minus))
        tdlp = t1+ datetime.timedelta(minutes= int(timedelta_plus))
        hostname = df['_source.host.name'][i]
        query1 = '{"_source":{"includes":["@timestamp","host.name","system.process.name","system.process.memory.rss.pct","system.process.cpu.total.pct"]},"sort": [{"system.process.memory.rss.pct": {"order": "desc"}}],"size": 3,"query":{"bool":{"must": [{"term":{"host.name":"'+hostname+'" }},{"range":{"@timestamp":{"lte":"'+tdlp.strftime("%Y-%m-%d"'T'"%H:%M:%S")+'","gte":"'+tdlm.strftime("%Y-%m-%d"'T'"%H:%M:%S")+'"}}},{"exists": {"field": "system.process.name"}}]}}}'

        data1 = es.search(index = indexname , body = (query1),scroll ='1m')
        df1 = json_normalize(data = data1['hits']['hits'],errors = 'ignore')

        for n in range(len(df1)):
            df_memory_utilization = df_memory_utilization.append(
                {"Timestamp": df1["_source.@timestamp"][n],"Hostname":df1["_source.host.name"][n],"ProcessName":df1["_source.system.process.name"][n],
                 "Process_Memory_Utilization":df1["_source.system.process.memory.rss.pct"][n],"CPU_utilization_per_process":df1["_source.system.process.cpu.total.pct"][n]},ignore_index=True)

    # Change data frame rows into dict (json ) format.
    df_docs = df_memory_utilization.to_dict(orient='records')
    # Write the information into an index
    bulk(es2, df_docs, index=new_indexname, doc_type='memeory_utilization', raise_on_error=True)
    df_memory_utilization.to_csv('memory_utilization_tested.csv')

    return 0
# ****************************************************************************************************************************************************
# *****************************************************************************************************************************************************
# **************************************************************CPU_UTILIZATION**************************************************************************

def cpu_utilization(start_time,end_time, indexname,percentfilter,timedelta_minus,timedelta_plus,new_indexname):
    # Empty Data frame
    df_cpu_utilization = pd.DataFrame(
        columns=["Timestamp", "Hostname", "Total_CPU_Utilization", "Total_Memory_Utilization", "ProcessName",
                 "Process_Memory_Utilization", "Process_CPU_Utilization"])
#  **********************************************************Total_CPU_Utilization > 90%****************************************************************

    # Query to retrieve system cpu utilization greater than a given percentage
    query2 ='{"size":100,"_source":{"includes":["@timestamp","host.name","system.cpu.total.pct"]},"query":{"bool": {"must":[{"range" : {"system.cpu.total.pct" : {"gte":"'+percentfilter+'"}}},{"range" : {"@timestamp":{"lte":"'+end_time+'","gte":"'+start_time+'"}}},{"exists": {"field":"system.cpu.total.pct"}}]}}}'
    # Python request to elastic search
    data = es.search(index=indexname, body=(query2), scroll='1m')
    # elastic search response to data frame
    df = json_normalize(data=data['hits']['hits'], errors='ignore')
    count = df['_source.@timestamp'].count()
    # .strftime("%Y-%m-%d"'T'"%H:%M:%S")
    # for loop to append data frame
    for i in range(count):
        df_cpu_utilization = df_cpu_utilization.append({"Timestamp":df['_source.@timestamp'][i],"Hostname": df['_source.host.name'][i],"Total_CPU_Utilization":df['_source.system.cpu.total.pct'][i]}, ignore_index=True)

# *********************************************************Total_Memory_Utilization************************************************************

    # for loop to create timestamp window by adding and subtracting a given time delta from
    # the time cpu utilization is greater than the requested percentage.
    for i in range(count):
        # timedelta_minus = pd.to_datetime(df['_source.@timestamp'][i]) - datetime.timedelta(minutes=5)
        # timedelta_plus = pd.to_datetime(df['_source.@timestamp'][i]) + datetime.timedelta(minutes=5)
        t1 = pd.to_datetime(df['_source.@timestamp'][i])

        tdlm = t1 - datetime.timedelta(minutes=int(timedelta_minus))
        tdlp = t1 + datetime.timedelta(minutes=int(timedelta_plus))
        hostname = df['_source.host.name'][i]
        # query to retrieve total memory utilization for the requested time window
        query0 = '{"_source":{"includes":["@timestamp","host.name","system.memory.used.pct"]},"sort": [{"system.memory.used.pct": {"order": "desc"}}],"size": 3,"query":{"bool": {"must": [{"term":{"host.name":"'+ hostname +'"}},{"range" : {"@timestamp" :  {"gte": "'+tdlm.strftime("%Y-%m-%d"'T'"%H:%M:%S") + '", "lte":"'+ tdlp.strftime("%Y-%m-%d"'T'"%H:%M:%S")+'"}}},{"exists": {"field": "system.memory.used.pct"}}]}}}'
        # query response to data frame
        data0 = es.search(index=indexname, body=(query0), scroll='1m')
        df0 = json_normalize(data=data0['hits']['hits'],errors='ignore')
        # for loop to append data frame
        for n in range(len(df0)):
            df_cpu_utilization = df_cpu_utilization.append(
            {"Timestamp":df0["_source.@timestamp"][n] , "Hostname":df0["_source.host.name"][n],
             "Total_Memory_Utilization":df0["_source.system.memory.used.pct"][n]}, ignore_index=True)

# ***********************************************************Process_CPU_Memory_Utilization*******************************************************

    # for loop to retrieve process information for the requested time window
    for i in range(count):
        t1 = pd.to_datetime(df['_source.@timestamp'][i])

        tdlm = t1 - datetime.timedelta(minutes=int(timedelta_minus))
        tdlp = t1 + datetime.timedelta(minutes=int(timedelta_plus))
        hostname = df['_source.host.name'][i]
        query1 = '{"_source":{"includes": ["@timestamp","host.name","system.process.name","system.process.memory.rss.pct","system.process.cpu.total.pct"]},"sort": [{"system.process.cpu.total.pct": {"order": "desc"}}],"size": 3,"query": {"bool": {"must": [{"term": {"host.name": "' + hostname + '"}},{"range": {"@timestamp": {"gte": "' + tdlm.strftime("%Y-%m-%d"'T'"%H:%M:%S") + '", "lte":"' + tdlp.strftime("%Y-%m-%d"'T'"%H:%M:%S") + '"}}},{"exists": {"field": "system.process.name"}}]}}}'

        data1 = es.search(index=indexname, body=(query1), scroll='1m')
        df1 = json_normalize(data=data1['hits']['hits'], errors='ignore')
        for n in range(len(df1)):
           df_cpu_utilization = df_cpu_utilization.append({"Timestamp":df1["_source.@timestamp"][n],"Hostname":df1["_source.host.name"][n],"ProcessName":df1["_source.system.process.name"][n], "Process_CPU_Utilization":df1["_source.system.process.cpu.total.pct"][n], "Process_Memory_Utilization":df1["_source.system.process.memory.rss.pct"][n]}, ignore_index=True)

    df_cpu_utilization.to_csv('CPU_utilization_tested.csv')
    # Change data frame rows into dict (json ) format.
    df_docs = df_cpu_utilization.to_dict(orient='records')
    # Write the information into an index
    bulk(es2, df_docs,index=new_indexname,doc_type='CPU_Utilization', raise_on_error=True)
    return 0

# function calls
memory_utilization('now-96h','now','metricbeat*','0.9000','5','5')


def main():
#     function call with system arguments to invoke the methods from java
      memory_utilization(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6])
      cpu_utilization(sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6])

#
 # invoking the main
if __name__ == "__main__":
      main()