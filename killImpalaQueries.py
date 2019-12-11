#!/usr/bin/python

## *******************************************************************************************
##  killLongRunningImpalaQueries.py
##
##  Kills Long Running Impala Queries
##
##  Usage: ./killLongRunningImpalaQueries.py  queryRunningSeconds [KILL]
##
##    Set queryRunningSeconds to the threshold considered "too long"
##    for an Impala query to run, so that queries that have been running 
##    longer than that will be identifed as queries to be killed
##
##    Set the CM URL, Cluster Name, login and password in the settings below
##
##    This script assumes there is only a single Impala service per cluster
##
## *******************************************************************************************


## ** imports *******************************

import sys
from datetime import datetime, timedelta
from cm_api.api_client import ApiResource
import ConfigParser

## ** Settings ******************************
Config = ConfigParser.ConfigParser()
Config.read("config")
## Cloudera Manager Host
cm_host = Config.get('CMLoginInfo', 'cmHost')
cm_port = Config.get('CMLoginInfo', 'cmPort')

## Cloudera Manager login with Full Administrator role
cm_login = Config.get('CMLoginInfo', 'cmUser')

## Cloudera Manager password
cm_password = Config.get('CMLoginInfo', 'cmPasswd')

## Cluster Name
cluster_name = Config.get('ClusterInfo', 'clusterName')

## Query User
superUserList = Config.get('QueryUser', 'ImpalaQuerySuperUser').split(',')
forbidenUserList = Config.get('QueryUser', 'ImpalaQueryForbidenUser').split(',')
unionUserList = superUserList + forbidenUserList

## Query Running Time
query_running_time = Config.get('QueryRunningTime', 'queryRunningTime')

## *****************************************
fmt = '%Y-%m-%d %H:%M:%S %Z'

## Connect to CM
print "\nConnecting to Cloudera Manager at " + cm_host + ":" + cm_port
api = ApiResource(server_host=cm_host, server_port=cm_port, username=cm_login, password=cm_password)

## Get the Cluster 
cluster = api.get_cluster(cluster_name)

## Get the IMPALA service
impala_service = None

service_list = cluster.get_all_services()
for service in service_list:
  if service.type == "IMPALA":
    impala_service = service
    print "Located Impala Service: " + service.name
    break
  
if impala_service is None:
  print "Error: Could not locate Impala Service"
  quit(1)

## A window of one day assumes queries have not been running more than 24 hours
now = datetime.utcnow()
start = now - timedelta(days=1)

## Get All the Running Impala Queries
impala_query_response = impala_service.get_impala_queries(start_time=start, end_time=now, limit=1000)
queries = impala_query_response.queries

## Get Full Query User List & Remove Duplicated
queryUser = ''
for i in range (0, len(queries)):
    query = queries[i]
    queryUser = queryUser + ',' + query.user
queryUserList = queryUser[1:].split(',')
queryUserList = list(set(queryUserList))

## Get Limited Query User List
limitedUserList = [item for item in queryUserList if item not in unionUserList]

## Filter String
filterStr1 = 'queryDuration > ' + query_running_time

filterStr2 = ''
for i in range (0, len(forbidenUserList)):
    filterStr2 = filterStr2 + 'user = ' + forbidenUserList[i] + ' OR '

filterStr3 = ''
for i in range (0, len(limitedUserList)):
    filterStr3 = filterStr3 + 'user = ' + limitedUserList[i] + ' OR '

## Kill Impala Queries Excuted by Forbiden Users
impala_query_forbiden_user_resp = impala_service.get_impala_queries(start_time=start, end_time=now, filter_str=filterStr2[:-3], limit=1000)
forbiden_queries = impala_query_forbiden_user_resp.queries

for i in range (0, len(forbiden_queries)):
  query = forbiden_queries[i]

  if query.queryState != 'FINISHED' and query.queryState != 'EXCEPTION':
    print '-- Kill Impala queries executed by the forbiden users ------------'
    print "queryState : " + query.queryState
    print "queryId: " + query.queryId 
    print "user: " + query.user
    print "startTime: " + query.startTime.strftime(fmt)
    query_duration = now - query.startTime
    print "query running time (seconds): " + str(query_duration.seconds + query_duration.days * 86400)
    print "SQL: " + query.statement
    print "Attempting to kill query..."
    impala_service.cancel_impala_query(query.queryId)
      
    print '---------------Done----------------------'


## Kill Impala Queries Excuted by Limited Users & Configured Running Time Threshold
impala_query_limited_user_resp = impala_service.get_impala_queries(start_time=start, end_time=now, filter_str=filterStr3[:-3] + ' AND ' + filterStr1, limit=1000)
limited_queries = impala_query_limited_user_resp.queries

for i in range (0, len(limited_queries)):
  query = limited_queries[i]

  if query.queryState != 'FINISHED' and query.queryState != 'EXCEPTION':
    
    print '-- Kill long running queries executed by the limited users-------------'
    print "queryState : " + query.queryState
    print "queryId: " + query.queryId 
    print "user: " + query.user
    print "startTime: " + query.startTime.strftime(fmt)
    query_duration = now - query.startTime
    print "query running time (seconds): " + str(query_duration.seconds + query_duration.days * 86400)
    print "SQL: " + query.statement
    print "Attempting to kill query..."
    impala_service.cancel_impala_query(query.queryId)
      
    print '----------------Done---------------------'
