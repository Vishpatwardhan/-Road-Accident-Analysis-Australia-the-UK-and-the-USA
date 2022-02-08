import json
import pymongo
import re
import pandas as pd
import requests
import csv
import json
from bson import json_util
from pymongo import MongoClient

import os
import json
import xmltodict
from pymongo import MongoClient
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # <-- ADD THIS LINE
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors
#import psycopg2.extras as extras

class accidents_analysis_aus: 
    
    ##Function to delete the existing accidents database from mongodb
    def check_mongodb(database_name,client):
        dbnames = client.list_database_names()
        if database_name in dbnames:
            client.drop_database(database_name)
        print('Deleted successfully')
    
    def aus_convert(df_new,column_list):
        for columns in column_list:
            df_new[columns] = pd.to_numeric(df_new[columns])
            
    
    ##Function to create and delete the existing accidents database from postgresql
    def createpostgres_db(db_name,host_name,user_name,pwd):
        conn = psycopg2.connect(
        database = 'postgres',
        user = user_name,
        password = pwd,
        host = host_name,
        port = '5432'
        )
        
        if conn is not None:
            conn.autocommit = True
    
        # Creating a cursor object
            cursor = conn.cursor()
    
            cursor.execute("SELECT datname FROM pg_database;")
    
    
    
            list_database = cursor.fetchall()
    
            if (db_name,) in list_database: 
                killdbuse_query = "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname ='" +db_name+"';"
                print(killdbuse_query)
                cursor.execute(killdbuse_query)
                print('Postgres existing db killed')
    
    #Drop existing database
                drop_sql_query = ''' DROP DATABASE '''+ db_name+''';''';
                cursor.execute(drop_sql_query)
    # executing above query
                print("Postgres sql Database has been deleted successfully !!");
    
    
    # query to create a database
            create_db_sql_query = ''' CREATE DATABASE '''+ db_name+''';''';
            print(create_db_sql_query)
            cursor.execute(create_db_sql_query)
    # executing above query
            print("Postgres sql Database has been created successfully !!");
    
    # Closing the connection
        conn.close()
        
    
    
    def create_postgres_accidents_schema(table_name,db_name,host_name,user_name,pwd):
    # connection establishment
        conn = psycopg2.connect(
            database = db_name,
            user = user_name,
            password = pwd,
            host = host_name,
            port = '5432'
            )
    
        if conn is not None:
            conn.autocommit = True
    
        # Creating a cursor object
            cursor = conn.cursor()
    
        # #Droping table if already exists.
        #     cursor.execute("DROP TABLE IF EXISTS casualties")
    
        #Droping table if already exists.
            cursor.execute("DROP TABLE IF EXISTS " + table_name)
    
        #Drop existing database
        #create_sql_query = '''CREATE TABLE accidents ( AccidentIndex VARCHAR(20) PRIMARY KEY, Longitude DECIMAL(9,6), Latitude DECIMAL(9,6), AccidentSeverity VARCHAR(20), NumberofVehicles INT, NumberofCasualties INT, DATE date, DayofWeek VARCHAR(20), Time VARCHAR(6),Speedlimit INT, LightConditions VARCHAR(50), WeatherConditions VARCHAR(50), RoadSurfaceConditions VARCHAR(50), UrbanorRuralArea VARCHAR(20) );'''
            create_sql_query = "CREATE TABLE australia_accidents ( id VARCHAR(10) PRIMARY KEY, Crash_ID VARCHAR(20),State VARCHAR(20),Month INT,Year VARCHAR(6),Day VARCHAR(10),Time VARCHAR(8),Crash_Type VARCHAR(20),Bus_Involvement VARCHAR(10),Heavy_Rigid_Truck_Involvement VARCHAR(6),Articulated_Truck_Involvement VARCHAR(6),Speed_Limit INT, Road_User VARCHAR(20), Gender VARCHAR(8),Age INT, Christmas_Period VARCHAR(5),Easter_Period VARCHAR(5),Age_Group VARCHAR(10), Day_of_week VARCHAR(12), Time_of_day VARCHAR(10) );"
            print(create_sql_query)
            cursor.execute(create_sql_query)
            print("this is after creating table")
        conn.close()
    
    def create_postgres_latitude_schema(table_title,db_name,host_name,user_name,pwd):
    # connection establishment
        conn = psycopg2.connect(
            database = db_name,
            user = user_name,
            password = pwd,
            host = host_name,
            port = '5432'
            )
    
        if conn is not None:
            conn.autocommit = True
    
        # Creating a cursor object
            cursor = conn.cursor()
    
        #Droping table if already exists.
            cursor.execute("DROP TABLE IF EXISTS casualties")
    
        #Droping table if already exists.
            cursor.execute("DROP TABLE IF EXISTS " + table_title)
    
        #Drop existing database
        
            create_sql_query = "CREATE TABLE australia_latitude ( No INT PRIMARY KEY, country VARCHAR(12),iso2 VARCHAR(6),State VARCHAR(6),latitude DECIMAL(9,6),longitude DECIMAL(9,6));"
            print(create_sql_query)
            cursor.execute(create_sql_query)
            print("this is after creating table")
        conn.close()
    
    def insert_aus_into_postgres(df,db_name,table_name,host_name,user_name,pwd):
            engine = create_engine('postgresql://'+user_name+':'+pwd+'@'+host_name+':5432/'+db_name,echo=False)
            # print('Value Pass')
        # Writing Dataframe to PostgreSQL and replacing table if it already exists
            df.to_sql(name=table_name, con=engine, if_exists = "replace", index=False)
            # print('Value Pass')
            
    def insert_lat_into_postgres(df1,db_name,table_title,host_name,user_name,pwd):
            engine = create_engine('postgresql://'+user_name+':'+pwd+'@'+host_name+':5432/'+db_name,echo=False)
            # print('Value Pass')
        # Writing Dataframe to PostgreSQL and replacing table if it already exists
            df1.to_sql(name=table_title, con=engine, if_exists = "replace", index=False)
            # print('Value Pass')
    #--------------------------Main Code----------------------------------
    
    #conda install -y pymongo
    #pip install requests
    
    
    
    #  To Fetch Data from API and To handle exception incase occurs
    response=requests.get("https://data.gov.au/data/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%22fd646fdc-7788-4bea-a736-e4aeb0dd09a8%22")
    print('Please wait as huge amounts of data is getting fetched from API...')
    try:
        response=requests.get("https://data.gov.au/data/api/3/action/datastore_search_sql?sql=SELECT%20*%20from%20%22fd646fdc-7788-4bea-a736-e4aeb0dd09a8%22")
    except ConnectionError:
        requests.exceptions.ConnectionError
        print("Internet connection failed")
    except requests.exceptions.HTTPError:
        print("URL is broken")
    except requests.exceptions.Timeout:
        print("Timeout Occured")
    response.text
    
    #Converting text data to json object
    json_data=json.loads(response.text)
    
    # Opening JSON file
    f = open('db_credentials.json')
    # returns JSON object as
    # a dictionary
    data = json.load(f)
    mongodb_userid = data['mongodb_credentials']['userid']
    mongodb_pwd = data['mongodb_credentials']['password']
    
    postgres_userid = data['postgres_credentials']['userid']
    postgres_pwd = data['postgres_credentials']['password']
    
    client = MongoClient("mongodb://%s:%s@127.0.0.1" % (mongodb_userid, mongodb_pwd))
    
    # To connect with accidents_aus database
    db=client.accidents_db
    
    database_name='accidents_db'
    #check_mongodb(database_name,client)
    
    
    
    #json_data["result"]["records"]
    
    # To clear out excess unnecessary data from json object, hence I have only selected result and records fields
    
    df_xyz=pd.DataFrame(json_data["result"]["records"])
    before_data=df_xyz.to_dict('records')
    collection=db.before_data
    x=collection.insert_many(before_data)
    
    #To drop db to avoid duplication
    #db_exists = 'accidents_aus' in client.list_database_names()
    #print(db_exists)
    #if db_exists == True:
    #    client.drop_database('accidents_aus')
    
    # To fetch data from mongodb and load it into dataframe
    
    Res=collection.find()
    df_new=pd.DataFrame(Res)
    print(df_new)
    df_new.info
    
    #cleaning of data:
    
    #1. To drop useless columns from dataframe
    
    df_new=df_new.drop(columns=['_full_text','National Remoteness Areas','SA4 Name 2016','National LGA Name 2017','National Road Type'])
    
    # 2. To check null values in data
    
    df_new.isna().sum()
    df_new['_id']
    
    # 3.
    
    # Value in Heavy Rigid Truck involvement is negative so need to drop it
    
    df_new.drop(df_new.index[df_new['Heavy Rigid Truck Involvement'] == '-9'], inplace=True)
    
    # 4.
    
    # Value of Articulated Truck Involvement cannot be negative
    df_new.drop(df_new.index[df_new['Articulated Truck Involvement'] == '-9'], inplace=True)
    
    #5.
    
    # Value of Bus Involvement cannot be negative hence need to drop it
    
    df_new.drop(df_new.index[df_new['Bus Involvement'] == "-9"], inplace=True)
    
    #6.
    
    #speed limit cannot be negative so need to drop it
    #need to remove unnecessary rows from column
    #need to remove text values in columns
    
    df_new.drop(df_new.index[df_new['Speed Limit'] == "<40"],inplace=True)
    
    df_new.drop(df_new.index[df_new['Speed Limit'] == "Unspecified"],inplace=True)
    
    df_new.drop(df_new.index[df_new['Speed Limit'] == "-9"], inplace=True)
    
    #7. Gender should not contain numeric values
    
    df_new.drop(df_new.index[df_new['Gender'] == "-9"], inplace=True)
    
    
    #changing name of few columns, its a best practise to remove spaces from column names before storing it in postgresql
    
    
    df_new.rename(columns={'_id': 'id','Crash ID': 'Crash_ID','Crash Type': 'Crash_Type','Bus Involvement': 'Bus_Involvement','Heavy Rigid Truck Involvement':'Heavy_Rigid_Truck_Involvement','Articulated Truck Involvement':'Articulated_Truck_Involvement','Speed Limit':'Speed_Limit','Road User':'Road_User','Christmas Period':'Christmas_Period','Easter Period':'Easter_Period','Age Group':'Age_Group','Dayweek':'Day','Time of day':'Time_of_day','Day of week':'Day_of_week'}, inplace=True)
    df_new
    
    # To change datatype of columns using function
    
    column_list=["id","Crash_ID","Year","Speed_Limit","Age"]
    aus_convert(df_new,column_list)
    
    #To fetch Latitude and Longitude of different states of Australia from csv file
    import csv
    import json
    
    #CSV to dataframe using pandas
    
    import pandas as pd
    df_lat=pd.read_csv(r'au.csv')
    
    db=client.accidents_db
    aus_lat_data=df_lat.to_dict('records')
    lat_collection=db.aus_lat_data
    z=lat_collection.insert_many(aus_lat_data)
    
    Res1=lat_collection.find()
    df_new1=pd.DataFrame(Res1)
    print(df_new1)
    df_new1.info
    df_new1.dtypes
    
    # drop unnecessary columns and to rename the column names
    
    df_new1=df_new1.drop(columns=['_id'])
    
    df_new1.rename(columns={'lat': 'latitude','long':'longitude'},inplace=True)
    
    #various ways to check the size of dataframe
    df_new1
    df_new.shape[0]
    len(df_new.index)
    df_new[df_new.columns].count()
    df_new.dtypes
    
    # To cross check whether data has been cleaned or not
    #df_new.to_csv(r'D:\DAP\DAP Project\op.csv')
    db_name='accidents_db'
    host_name='localhost'
    #createpostgres_db(db_name,host_name,postgres_userid,postgres_pwd)
    
    #
    db_name='accidents_db'
    host_name='localhost'
    df=df_new
    df1=df_new1
    table_name='australia_accidents'
    table_title='australia_latitude'
    
    create_postgres_accidents_schema(table_name,db_name,host_name,postgres_userid,postgres_pwd)
    create_postgres_latitude_schema(table_title,db_name,host_name,postgres_userid,postgres_pwd)
    insert_aus_into_postgres(df,db_name,table_name,host_name,postgres_userid,postgres_pwd)
    insert_lat_into_postgres(df1,db_name,table_title,host_name,postgres_userid,postgres_pwd)
    
    # To create an engine instance
    conn = psycopg2.connect(database="accidents_db", user=postgres_userid, password=postgres_pwd, host='127.0.0.1', port= '5432')
    
        
    conn.autocommit = True
    
    #Intializing cursor object'
    
    cursor = conn.cursor()
    
    #Retrieving data in cursor to later on use for further processing
    cursor.execute('''SELECT * from australia_accidents''')
    
    #Fetching 1st row from the australia accident table into new dataframe
    df_austAccident = cursor.fetchall();
    cols = list(map(lambda x: x[0], cursor.description))
    df_austAccident = pd.DataFrame.from_records(df_austAccident, columns=cols)
    df_austAccident.head()
    
    #selecting entire data into cursor
    
    cursor.execute('''SELECT * from australia_latitude''')
    
    #Fetching 1st row from the latitude and longitude table into new dataframe
    df_austlatd = cursor.fetchall();
    cols = list(map(lambda x: x[0], cursor.description))
    df_austlatd = pd.DataFrame.from_records(df_austlatd, columns=cols)
    df_austlatd.head()
    
    # Python do not auto commit by default, hence need to manually commit to intiate end of transaction
    conn.commit()
    
    # To disconnect the database
    conn.close()
    
    #Finally merging data into single frame using joins
    Aust_final_df= pd.merge(df_austAccident,df_austlatd,on=['State'],how='inner')
    Aust_final_df.head()
    Aust_final_df.dtypes
    
    from matplotlib import pyplot as plt
    a=Aust_final_df.groupby(['Year']).count()
    fig=plt.figure()
    fig.patch.set_facecolor('#DCDCDC')
    plt.plot(a['id'], color='#009DF1', label='Accidents',linewidth=2.5, marker='.', markersize=16, linestyle='--', markeredgecolor='black')
    plt.xlabel("Years", fontdict={'weight':'bold'})
    plt.ylabel("Total number of Accidents", fontdict={'weight':'bold'})
    plt.title('Year-wise Accident numbers', fontdict={'fontsize':15,'weight':'bold'})
    plt.grid(color = '#DCDCDC', linestyle = '--', linewidth = 1)
    plt.legend()
    plt.show()



