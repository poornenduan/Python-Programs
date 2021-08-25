from elasticsearch import Elasticsearch
from pandas import DataFrame
import json
import regex as re
es = Elasticsearch("http://40.122.111.128:5044")
es2 = Elasticsearch("http://localhost:9200")
from pandas.io.json import json_normalize
from elasticsearch.helpers import bulk
import pandas as pd
import sys
import datetime
from time import strftime
import ast
from pprint import pprint




print(datetime.datetime.now())
def ServerHistoricalPerformance(start_time,start_time1,end_time, indexname,indexname1,period):
    df_ServerHistoricalPerformance = pd.DataFrame(columns=["Timestamp", "Hostname", "metric_category", "Counts", "min", "max", "avg", "sum",
                               "variance", "std_deviation", "historical_avg", "historical_max",
                               "historical_std_deviation"])
    # query to get list of hostnames for the given period
    query1 = '{"aggs" : {"Hostname" :{"terms":{ "field" :"host.name","size": 10000}}}}'
    data = es.search(index= indexname, body=(query1), scroll='1m')
    df = json_normalize(data=data['aggregations']['Hostname']['buckets'], errors='ignore')
    count = df['key'].count()
    # for loop to get the CPU stats of list of hostnames for the given period.
    # print(count)
    for i in range(5):
        #**********************************************************CPU_STATS************************************************************************
        hostname = df['key'][i]
        query_CPU_1 = '{"query":{"bool": {"must":[{"term":{"host.name":"'+hostname+'"}},{"range" : {"@timestamp":{"lte":"'+end_time+'","gte":"'+start_time+'","format":"yyyy-MM-dd"}}}]}},"aggs" : {"CPU_stats" : { "extended_stats" : { "field" :"system.cpu.total.pct"  }}}}'
        data_CPU_1 = es.search(index=indexname, body=(query_CPU_1))
        df_CPU_1 = json_normalize(data=data_CPU_1['aggregations']['CPU_stats'], errors='ignore')
        query_CPU_2 = '{"query":{"bool": {"must":[{"term":{"host.name":"'+hostname+'"}},{"range" : {"@timestamp":{"lte":"' + end_time+'","gte":"' +end_time +'-'+period+'d''","format":"yyyy-MM-dd"}}}]}},"aggs" : {"CPU_stats" : { "extended_stats" : { "field" :"system.cpu.total.pct"  }}}}'
        data_CPU_2 = es.search(index=indexname, body=(query_CPU_2))
        df_CPU_2 = json_normalize(data=data_CPU_2['aggregations']['CPU_stats'], errors='ignore')
        if (end_time == 'now'):
            current = datetime.datetime.now()
            current = current.strftime("%Y-%m-%d %H:%M")

        else:
            current = pd.to_datetime(end_time)
        # move CPU stats information to the dataframe
        df_ServerHistoricalPerformance = df_ServerHistoricalPerformance.append({"Timestamp": current ,"Hostname": hostname,
             "metric_category":"CPU_Utilizatization","Counts": df_CPU_1['count'],"min":df_CPU_1['min'],"max":df_CPU_1['max'],
            "avg":df_CPU_1['avg'],"sum":df_CPU_1['sum'],"variance":df_CPU_1['variance'],
            "std_deviation" : df_CPU_1['std_deviation'],"historical_avg": df_CPU_2['avg'],
            "historical_max": df_CPU_2['max'],"historical_std_deviation": df_CPU_2['std_deviation']},ignore_index=True)

 #       ***************************************************************MEM_STATS***********************************************************************
         # repeating the steps above to get Memory information for a list of hostnames

        query_MEM_1 = '{"query":{"bool": {"must":[{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + start_time + '","format":"yyyy-MM-dd"}}}]}},"aggs" : {"Memory_stats" : { "extended_stats" : { "field" :"system.memory.used.pct"  }}}}'
        data_MEM_1 = es.search(index=indexname, body=(query_MEM_1), scroll='1m')
        df_MEM_1 = json_normalize(data=data_MEM_1['aggregations']['Memory_stats'], errors='ignore')
        query_MEM_2 = '{"query":{"bool": {"must":[{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + end_time + '-' + period + 'd' + '","format":"yyyy-MM-dd"}}}]}},"aggs" : {"Memory_stats" : { "extended_stats" : { "field" :"system.memory.used.pct"  }}}}'
        data_MEM_2 = es.search(index=indexname, body=(query_MEM_2), scroll='1m')
        df_MEM_2 = json_normalize(data=data_MEM_2['aggregations']['Memory_stats'], errors='ignore')
        if (end_time == 'now'):
            current = datetime.datetime.now()
            current = current.strftime("%Y-%m-%d %H:%M")

        else:
            current = pd.to_datetime(end_time)
        df_ServerHistoricalPerformance = df_ServerHistoricalPerformance.append(
            {"Timestamp": current, "Hostname": hostname, "metric_category": "Memory_Utilization",
             "Counts": df_MEM_1['count'], "min": df_MEM_1['min'], "max": df_MEM_1['max'],
             "avg": df_MEM_1['avg'], "sum": df_MEM_1['sum'],
             "variance": df_MEM_1['variance'], "std_deviation": df_MEM_1['std_deviation'],
             "historical_avg": df_MEM_2['avg'], "historical_max": df_MEM_2['max'],
             "historical_std_deviation": df_MEM_2['std_deviation']}, ignore_index=True)

        #***************************************************************LOGIN_FAILURE***********************************************************
        # repeating the steps above to get login failure information for a list of hostnames
    # query2 = '{"query":{"bool":{"must":[{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + start_time + '"}}}]}},"aggs" : {"Hostname" :{"terms":{ "field" :"host.name","size": 10000}}}}'
    # data = es.search(index=indexname1, body=(query2), scroll='1m')
    # df = json_normalize(data=data['aggregations']['Hostname']['buckets'], errors='ignore')
    # count = df['key'].count()
    # # for loop to get the CPU stats of list of hostnames for the given period.
    # print(count)
    # for i in range(count):
        query_LF_1 = '{"query":{"bool": {"must":[{"term":{"event_id":"4625"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + start_time1 + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_LF_1 = es.search(index=indexname1, body=(query_LF_1))
        df_LF_1 = json_normalize(data=data_LF_1['aggregations']['stats_events_per_hour'], errors='ignore')
        query_LF_2 = '{"query":{"bool": {"must":[{"term":{"event_id":"4625"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + end_time + '-' + period + 'd' + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_LF_2 = es.search(index=indexname1, body=(query_LF_2))
        df_LF_2 = json_normalize(data=data_LF_2['aggregations']['stats_events_per_hour'], errors='ignore')
        if (end_time == 'now'):
            current = datetime.datetime.now()
            current = current.strftime("%Y-%m-%d %H:%M")

        else:
            current = pd.to_datetime(end_time)
        df_ServerHistoricalPerformance = df_ServerHistoricalPerformance.append(
            {"Timestamp": current, "Hostname": hostname, "metric_category": "Login_Failure",
             "Counts": df_LF_1['count'], "min": df_LF_1['min'], "max": df_LF_1['max'],
             "avg": df_LF_1['avg'], "sum": df_LF_1['sum'],
             "variance": df_LF_1['variance'], "std_deviation": df_LF_1['std_deviation'],
             "historical_avg": df_LF_2['avg'], "historical_max": df_LF_2['max'],
             "historical_std_deviation": df_LF_2['std_deviation']}, ignore_index=True)


   # ****************************************************************System Errors******************************************************************
     #   repeating the steps above to get System Errors information for a list of hostnames
        query_SE_1 = '{"query":{"bool": {"must":[{"term":{"log_name":"System"}},{"term":{"level":"Error"}},{"term":{"host.name":"HAMMON002"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + start_time1 + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_SE_1 = es.search(index=indexname, body=(query_SE_1))
        df_SE_1 = json_normalize(data=data_SE_1['aggregations']['stats_events_per_hour'], errors='ignore')
        query_SE_2 = '{"query":{"bool": {"must":[{"term":{"log_name":"System"}},{"term":{"level":"Error"}},{"term":{"host.name":"HAMMON002"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + end_time + '-' + period + 'd' + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_SE_2 = es.search(index=indexname, body=(query_SE_2))
        df_SE_2 = json_normalize(data=data_SE_2['aggregations']['stats_events_per_hour'], errors='ignore')
        if (end_time == 'now'):
            current = datetime.datetime.now()
            current = current.strftime("%Y-%m-%d %H:%M")

        else:
            current = pd.to_datetime(end_time)
        df_ServerHistoricalPerformance = df_ServerHistoricalPerformance.append(
            {"Timestamp": current, "Hostname": hostname, "metric_category": "System Error",
             "Counts": df_SE_1['count'], "min": df_SE_1['min'], "max": df_SE_1['max'],
             "avg": df_SE_1['avg'], "sum": df_SE_1['sum'],
             "variance": df_SE_1['variance'], "std_deviation": df_SE_1['std_deviation'],
             "historical_avg": df_SE_2['avg'], "historical_max": df_SE_2['max'],
             "historical_std_deviation": df_SE_2['std_deviation']}, ignore_index=True)

    #*********************************************************************System warnings***********************************************************

        # repeating the steps above to get System Warnings information for a list of hostnames

        query_SW_1 = '{"query":{"bool": {"must":[{"term":{"log_name":"System"}},{"term":{"level":"Warning"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + start_time1 + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_SW_1 = es.search(index=indexname1, body=(query_SW_1))
        df_SW_1 = json_normalize(data=data_SW_1['aggregations']['stats_events_per_hour'], errors='ignore')
        query_SW_2 = '{"query":{"bool": {"must":[{"term":{"log_name":"System"}},{"term":{"level":"Warning"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + end_time + '-' + period + 'd' + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_SW_2 = es.search(index=indexname1, body=(query_SW_2))
        df_SW_2 = json_normalize(data=data_SW_2['aggregations']['stats_events_per_hour'], errors='ignore')
        if (end_time == 'now'):
            current = datetime.datetime.now()
            current = current.strftime("%Y-%m-%d %H:%M")

        else:
            current = pd.to_datetime(end_time)
        df_ServerHistoricalPerformance = df_ServerHistoricalPerformance.append(
            {"Timestamp": current, "Hostname": hostname, "metric_category": "System Warnings",
             "Counts": df_SW_1['count'], "min": df_SW_1['min'], "max": df_SW_1['max'],
             "avg": df_SW_1['avg'], "sum": df_SW_1['sum'],
             "variance": df_SW_1['variance'], "std_deviation": df_SW_1['std_deviation'],
             "historical_avg": df_SW_2['avg'], "historical_max": df_SW_2['max'],
             "historical_std_deviation": df_SW_2['std_deviation']}, ignore_index=True)


    #******************************************************************Application Errors***********************************************************

        # repeating the steps above to get Application Errors information for a list of hostnames

        query_AE_1 = '{"query":{"bool": {"must":[{"term":{"log_name":"Application"}},{"term":{"level":"Error"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + start_time1 + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_AE_1 = es.search(index=indexname1, body=(query_AE_1))
        df_AE_1 = json_normalize(data=data_AE_1['aggregations']['stats_events_per_hour'], errors='ignore')
        query_AE_2 = '{"query":{"bool": {"must":[{"term":{"log_name":"Application"}},{"term":{"level":"Error"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + end_time + '-' + period + 'd' + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_AE_2 = es.search(index=indexname1, body=(query_AE_2))
        df_AE_2 = json_normalize(data=data_AE_2['aggregations']['stats_events_per_hour'], errors='ignore')
        if (end_time == 'now'):
            current = datetime.datetime.now()
            current = current.strftime("%Y-%m-%d %H:%M")

        else:
            current = pd.to_datetime(end_time)
        df_ServerHistoricalPerformance = df_ServerHistoricalPerformance.append(
            {"Timestamp": current, "Hostname": hostname, "metric_category": "Application Error",
             "Counts": df_AE_1['count'], "min": df_AE_1['min'], "max": df_AE_1['max'],
             "avg": df_AE_1['avg'], "sum": df_AE_1['sum'],
             "variance": df_AE_1['variance'], "std_deviation": df_AE_1['std_deviation'],
             "historical_avg": df_AE_2['avg'], "historical_max": df_AE_2['max'],
             "historical_std_deviation": df_AE_2['std_deviation']}, ignore_index=True)


    #*********************************************************************Application warnings*******************************************************
        # repeating the steps above to get Application Warnings information for a list of hostnames


        query_AW_1 = '{"query":{"bool": {"must":[{"term":{"log_name":"Application"}},{"term":{"level":"Warning"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + start_time1 + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_AW_1 = es.search(index=indexname1, body=(query_AW_1))
        df_AW_1 = json_normalize(data=data_AW_1['aggregations']['stats_events_per_hour'], errors='ignore')
        query_AW_2 = '{"query":{"bool": {"must":[{"term":{"log_name":"Application"}},{"term":{"level":"Warning"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + end_time + '-' + period + 'd' + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_AW_2 = es.search(index=indexname1, body=(query_AW_2))
        df_AW_2 = json_normalize(data=data_AW_2['aggregations']['stats_events_per_hour'], errors='ignore')
        if (end_time == 'now'):
            current = datetime.datetime.now()
            current = current.strftime("%Y-%m-%d %H:%M")

        else:
            current = pd.to_datetime(end_time)
        df_ServerHistoricalPerformance = df_ServerHistoricalPerformance.append(
            {"Timestamp": current, "Hostname": hostname, "metric_category": "Application Warnings",
             "Counts": df_AW_1['count'], "min": df_AW_1['min'], "max": df_AW_1['max'],
             "avg": df_AW_1['avg'], "sum": df_AW_1['sum'],
             "variance": df_AW_1['variance'], "std_deviation": df_AW_1['std_deviation'],
             "historical_avg": df_AW_2['avg'], "historical_max": df_AW_2['max'],
             "historical_std_deviation": df_AW_2['std_deviation']}, ignore_index=True)

    #**************************************************************Security Errors******************************************************************\
        # repeating the steps above to get Security Errors information for a list of hostnames

        query_SeE_1 = '{"query":{"bool": {"must":[{"term":{"log_name":"Security"}},{"term":{"level":"Error"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + start_time1 + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_SeE_1 = es.search(index=indexname1, body=(query_SeE_1))
        df_SeE_1 = json_normalize(data=data_SeE_1['aggregations']['stats_events_per_hour'], errors='ignore')
        query_SeE_2 = '{"query":{"bool": {"must":[{"term":{"log_name":"Security"}},{"term":{"level":"Error"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + end_time + '-' + period + 'd' + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_SeE_2 = es.search(index=indexname1, body=(query_SeE_2))
        df_SeE_2 = json_normalize(data=data_SeE_2['aggregations']['stats_events_per_hour'], errors='ignore')
        if (end_time == 'now'):
            current = datetime.datetime.now()
            current = current.strftime("%Y-%m-%d %H:%M")

        else:
            current = pd.to_datetime(end_time)
        df_ServerHistoricalPerformance = df_ServerHistoricalPerformance.append(
            {"Timestamp": current, "Hostname": hostname, "metric_category": "Security Error",
             "Counts": df_SeE_1['count'], "min": df_SeE_1['min'], "max": df_SeE_1['max'],
             "avg": df_SeE_1['avg'], "sum": df_SeE_1['sum'],
             "variance": df_SeE_1['variance'], "std_deviation": df_SeE_1['std_deviation'],
             "historical_avg": df_SeE_2['avg'], "historical_max": df_SeE_2['max'],
             "historical_std_deviation": df_SeE_2['std_deviation']}, ignore_index=True)

    #*****************************************************************Security warnings***************************************************************
        # repeating the steps above to get Security warnings information for a list of hostnames

        query_SeW_1 = '{"query":{"bool": {"must":[{"term":{"log_name":"Security"}},{"term":{"level":"Error"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + start_time1 + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_SeW_1 = es.search(index=indexname1, body=(query_SeW_1))
        df_SeW_1 = json_normalize(data=data_SeW_1['aggregations']['stats_events_per_hour'], errors='ignore')
        query_SeW_2 = '{"query":{"bool": {"must":[{"term":{"log_name":"Security"}},{"term":{"level":"Error"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + end_time + '-' + period + 'd' + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_SeW_2 = es.search(index=indexname1, body=(query_SeW_2))
        df_SeW_2 = json_normalize(data=data_SeW_2['aggregations']['stats_events_per_hour'], errors='ignore')
        if (end_time == 'now'):
            current = datetime.datetime.now()
            current = current.strftime("%Y-%m-%d %H:%M")

        else:
            current = pd.to_datetime(end_time)
        df_ServerHistoricalPerformance = df_ServerHistoricalPerformance.append(
            {"Timestamp": current, "Hostname": hostname, "metric_category": "Security Warnings",
             "Counts": df_SeW_1['count'], "min": df_SeW_1['min'], "max": df_SeW_1['max'],
             "avg": df_SeW_1['avg'], "sum": df_SeW_1['sum'],
             "variance": df_SeW_1['variance'], "std_deviation": df_SeW_1['std_deviation'],
             "historical_avg": df_SeW_2['avg'], "historical_max": df_SeW_2['max'],
             "historical_std_deviation": df_SeW_2['std_deviation']}, ignore_index=True)



    #***********************************************************************Account Locked***********************************************************

        # repeating the steps above to get Account locked information for a list of hostnames

        query_AL_1 = '{"query":{"bool": {"must":[{"term":{"event_id":"4740"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + start_time1 + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_AL_1 = es.search(index=indexname1, body=(query_AL_1))
        df_AL_1 = json_normalize(data=data_AL_1['aggregations']['stats_events_per_hour'], errors='ignore')
        query_AL_2 = '{"query":{"bool": {"must":[{"term":{"event_id":"4740"}},{"term":{"host.name":"' + hostname + '"}},{"range" : {"@timestamp":{"lte":"' + end_time + '","gte":"' + end_time + '-' + period + 'd' + '","format":"yyyy-MM-dd"}}}]}},"aggs": {"range_aggs": {"date_histogram": {"field": "@timestamp","interval": "hour","format": "yyyy-MM-dd HH:mm"},"aggs": {"events":{ "value_count" : { "field" : "event_id" }}}},"stats_events_per_hour": {"extended_stats_bucket": {"buckets_path": "range_aggs>events" }}}}'
        data_AL_2 = es.search(index=indexname1, body=(query_AL_2))
        df_AL_2 = json_normalize(data=data_AL_2['aggregations']['stats_events_per_hour'], errors='ignore')
        if (end_time == 'now'):
            current = datetime.datetime.now()
            current = current.strftime("%Y-%m-%d %H:%M")

        else:
            current = pd.to_datetime(end_time)
        df_ServerHistoricalPerformance = df_ServerHistoricalPerformance.append(
            {"Timestamp": current, "Hostname": hostname, "metric_category": "Account Locked",
             "Counts": df_AL_1['count'], "min": df_AL_1['min'], "max": df_AL_1['max'],
             "avg": df_AL_1['avg'], "sum": df_AL_1['sum'],
             "variance": df_AL_1['variance'], "std_deviation": df_AL_1['std_deviation'],
             "historical_avg": df_AL_2['avg'], "historical_max": df_AL_2['max'],
             "historical_std_deviation": df_AL_2['std_deviation']}, ignore_index=True)

    df_ServerHistoricalPerformance.to_csv('new_12.csv')

    # data cleansing steps
    df = df_ServerHistoricalPerformance
    if not es2.indices.exists("server_historical_performance_summary"):
          es2.indices.create("server_historical_performance_summary")
    # remove "Name : dtype" junk created in the df
    result = {"name": "Simple Example",
              "data": df, }

    jstr = json.dumps(result,
                      default=lambda df: json.loads(df.to_json()))
    newresult = json.loads(jstr)
    d = pd.DataFrame(newresult['data'])
    cnt = len(d)
    # change from dict format to value for stat columns eg: Counts :{'0':0} to Counts:0
    for i in range(3,13):
          for j in range(cnt):
              c = d[d.columns[i]][j]
              d[d.columns[i]][j] = c['0']
    data = d.to_dict(orient='records')
    print(data)
    bulk(es2, data, index='server_historical_performance_summary', doc_type='document', raise_on_error=True)
    print(datetime.datetime.now())


def main():

        ServerHistoricalPerformance('now','now-24h','now','*metricbeat-*','*winlogbeat-*','2')



#       invoking the main
if __name__ == "__main__":
    main()

