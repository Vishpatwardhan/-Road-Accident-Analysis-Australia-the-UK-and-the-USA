#!/usr/bin/env python
# coding: utf-8

# In[2]:


#--------------------------------Main Code--------------------------------------
import requests
from sodapy import Socrata
import pandas as pd
from pymongo import MongoClient, InsertOne
import json
import numpy as np
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
from psycopg2 import sql

import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # <-- ADD THIS LINE
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors

class USAccidents:
    
    ##Function to delete the existing accidents database from mongodb
    def check_mongodb(database_name,client):
        dbnames = client.list_database_names()
        if database_name in dbnames:
            client.drop_database(database_name)
    
    #Function to convert data types in the data frame
    
    def dfdatatype_convert(df_mongo_crashes,column_list):
        for columns in column_list:
            if columns!='crash_date':
                df_mongo_crashes[columns] = pd.to_numeric(df_mongo_crashes[columns])
            else:
                df_mongo_crashes[columns] = df_mongo_crashes[columns].str.replace('T',' ')
                df_mongo_crashes['date'] = df_mongo_crashes[columns].str[:10]
                df_mongo_crashes['year'] = df_mongo_crashes[columns].str[:4]
                
    def createpostgres_db(db_name,host_name,user_name,pwd):
    # connection establishment
        conn = psycopg2.connect(
            database = 'postgres',
            user = user_name,
            password = pwd,
            host = host_name,
            port = '5432'
            )
        print('a')
        if conn is not None:
            conn.autocommit = True
    
            # Creating a cursor object
            cursor = conn.cursor()
    
            cursor.execute("SELECT datname FROM pg_database;")
    
            list_database = cursor.fetchall()
    
            if (db_name,) in list_database: #delete existing database
                #Terminate use of the database
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
        
    
    def create_postgres_USaccidents_schema(table_name,db_name,host_name,user_name,pwd):
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
            cursor.execute("DROP TABLE IF EXISTS " + table_name)
    
            #Drop existing database
            #create_sql_query = '''CREATE TABLE accidents ( AccidentIndex VARCHAR(20) PRIMARY KEY, Longitude DECIMAL(9,6), Latitude DECIMAL(9,6), AccidentSeverity VARCHAR(20), NumberofVehicles INT, NumberofCasualties INT, DATE date, DayofWeek VARCHAR(20), Time VARCHAR(6),Speedlimit INT, LightConditions VARCHAR(50), WeatherConditions VARCHAR(50), RoadSurfaceConditions VARCHAR(50), UrbanorRuralArea VARCHAR(20) );'''
            create_sql_query = "CREATE TABLE accidents_us ( crash_record_id VARCHAR(200) PRIMARY KEY, age INT, sex VARCHAR(20), crash_date date, posted_speed_limit INT, traffic_control_device VARCHAR(50), device_condition VARCHAR(50), weather_condition VARCHAR(50), lighting_condition VARCHAR(50),first_crash_type VARCHAR(50), trafficway_type VARCHAR(50), alignment VARCHAR(50), roadway_surface_cond VARCHAR(50), road_defect VARCHAR(50), report_type VARCHAR(50), crash_type VARCHAR(50), damage VARCHAR(50), street_no VARCHAR(50), street_direction VARCHAR(20), street_name VARCHAR(50), beat_of_occurrence INT, crash_hour INT, crash_day_of_week INT, crash_month INT, latitude DECIMAL(9,6), longitude DECIMAL(9,6), location VARCHAR(50), num_units INT, injuries_total DECIMAL(9,6), injuries_fatal VARCHAR(20), injuries_incapacitating VARCHAR(20), injuries_non_incapacitating VARCHAR(20), injuries_reported_not_evident VARCHAR(20), injuries_no_indication VARCHAR(20));"
            print(create_sql_query)
            cursor.execute(create_sql_query)
            print("this is after creating table")
        conn.close()
        
    def create_postgres_UScasualty_schema(table_name2,db_name,host_name,user_name,pwd):
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
            cursor.execute("DROP TABLE IF EXISTS " + table_name2)
    
            #Drop existing database
            #create_sql_query = '''CREATE TABLE accidents ( AccidentIndex VARCHAR(20) PRIMARY KEY, Longitude DECIMAL(9,6), Latitude DECIMAL(9,6), AccidentSeverity VARCHAR(20), NumberofVehicles INT, NumberofCasualties INT, DATE date, DayofWeek VARCHAR(20), Time VARCHAR(6),Speedlimit INT, LightConditions VARCHAR(50), WeatherConditions VARCHAR(50), RoadSurfaceConditions VARCHAR(50), UrbanorRuralArea VARCHAR(20) );'''
            create_sql_query = "CREATE TABLE casualty_us ( crash_record_id VARCHAR(200) PRIMARY KEY, age INT, sex VARCHAR(20));"
            print(create_sql_query)
            cursor.execute(create_sql_query)
            print("this is after creating table")
        conn.close()
    
    
    
    def insert_dfUSaccidents_into_postgres(df,db_name,table_name,host_name,user_name,pwd):
        engine = create_engine('postgresql://'+user_name+':'+pwd+'@'+host_name+':5432/'+db_name,echo=False)
        # Writing Dataframe to PostgreSQL and replacing table if it already exists
        df.to_sql(name=table_name, con=engine, if_exists = 'replace', index=False)
    
    
    def insert_dfUScasualty_into_postgres(df,db_name,table_name,host_name,user_name,pwd):
        engine = create_engine('postgresql://'+user_name+':'+pwd+'@'+host_name+':5432/'+db_name,echo=False)
        # Writing Dataframe to PostgreSQL and replacing table if it already exists
        df.to_sql(name=table_name, con=engine, if_exists = 'replace', index=False)


    #Data fetch using API (Chicago Data Portal)

    c = Socrata("data.cityofchicago.org", None)
    c2 = Socrata("data.cityofchicago.org", None)
    print('Please wait as huge amounts of data is getting fetched from API...')
    #chicago_df = c.get("85ca-t3if", limit=600000)
    #API fetch with exception handling
    try:
        chicago_df = c.get("85ca-t3if", limit=600000)
    except requests.exceptions.ConnectionError:
       print("Internet Connectivity Error!!")
    except requests.exceptions.HTTPError:
       print("URL not found.")
    except requests.exceptions.Timeout:
       print("Server Connection Time out!")
    
    #chicagoPeople_df = c2.get("u6pd-qa9d",limit=1300000)
    try:
        chicagoPeople_df = c2.get("u6pd-qa9d",limit=1300000)
    except requests.exceptions.ConnectionError:
        print("Internet Connectivity Error!!")
    except requests.exceptions.HTTPError:
       print("Invalid URL!!")
    except requests.exceptions.Timeout:
       print("Server Connection Time out!")
    
    chicagoPeople_df = pd.DataFrame.from_records(chicagoPeople_df)
    chicagoPeople_df = pd.DataFrame(data=chicagoPeople_df, columns=["crash_record_id", "age","sex"])
    
    
    
    
    # Accessing MongoDb User credentials from JSON file
    
    f = open('db_credentials.json')
    
    
    # return JSON object as a dictionary
    
    data = json.load(f)
    mongodb_userid = data['mongodb_credentials']['userid']
    mongodb_pwd = data['mongodb_credentials']['password']
    
    postgres_userid = data['postgres_credentials']['userid']
    postgres_pwd = data['postgres_credentials']['password']
    
    
    #Write data in MongoDB
    
    df = pd.DataFrame.from_records(chicago_df)
    USaccident_rawData = df.to_dict('records')
    UScasualty_rawData=chicagoPeople_df.to_dict('records')
    
    client = MongoClient("mongodb://%s:%s@127.0.0.1" % (mongodb_userid, mongodb_pwd))
    myDB=client.accidents_db
    
    
    #To check if database exist in MongoDB and delete its instance
    
    database_name='accidents_db'
    #check_mongodb(database_name,client)
    
    
    #To write the database again in MongoDB after deleting its old instance
    
    collection = myDB.USaccident_rawData
    collection2 = myDB.UScasualty_rawData 
    x1=collection.insert_many(USaccident_rawData)
    x2=collection2.insert_many(UScasualty_rawData)
    print(x1)
    print(x2)
    
    
    #Read data from Mongo DB
    
    import pandas as pd
    find_mycol = collection.find()
    find_mycol2 = collection2.find()
    df_mongo_crashes=pd.DataFrame(find_mycol)
    df_mongo_people=pd.DataFrame(find_mycol2)
    df_mongo_crashes.head()
    df_mongo_people.head()
    
    
    #Drop unnecessary columns
    
    df_mongo_crashes=df_mongo_crashes.drop(columns=['_id','location', 'hit_and_run_i', 'most_severe_injury', 'injuries_unknown', 'statements_taken_i','crash_date_est_i','private_property_i','photos_taken_i','work_zone_i','work_zone_type','workers_present_i','dooring_i','lane_cnt','rd_no','intersection_related_i','date_police_notified','prim_contributory_cause','sec_contributory_cause'])
    df_mongo_crashes.head()
    
    df_mongo_people=df_mongo_people.drop(columns=['_id'])
    df_mongo_people.head()
    
    
    #Removing NaN and 0 values from specific columns
    
    df_mongo_crashes['injuries_total'].replace({'0': np.NaN}, inplace=True)
    df_mongo_crashes.dropna(subset = ['injuries_total'],inplace=True)
    
    df_mongo_crashes['latitude'].replace({'0': np.NaN}, inplace=True)
    df_mongo_crashes.dropna(subset = ['latitude'], inplace=True)
    df_mongo_crashes
    
    
    #Convert the datatype of necessary columns of the dataframe
    
    column_list=['posted_speed_limit','beat_of_occurrence','crash_hour','crash_day_of_week','crash_month','latitude','longitude','num_units','injuries_total','injuries_fatal','crash_day_of_week','crash_date']
    #df_mongo_crashes=df_mongo_crashes
    dfdatatype_convert(df_mongo_crashes,column_list)
    df_mongo_crashes.dtypes
    df_mongo_crashes.head()
    
    
    
    df_mongo_people = df_mongo_people.drop_duplicates('crash_record_id', keep='first')
    df_mongo_people['age'].replace({'0': np.NaN}, inplace=True)
    df_mongo_people.dropna(subset = ['age'],inplace=True)
    df_mongo_people.dropna(subset = ['sex'],inplace=True)
    df_mongo_people['age']=pd.to_numeric(df_mongo_people['age'])
    df_mongo_people.drop(df_mongo_people.index[df_mongo_people['age'] >= 95],inplace=True)
    
    
    #PostgreSQL data push
    
    db_name='accidents_db'
    host_name='localhost'
    table_name='accidents_us'
    table_name2='casualty_us'
    #createpostgres_db(db_name,host_name,postgres_userid,postgres_pwd)
    create_postgres_USaccidents_schema(table_name,db_name,host_name,postgres_userid,postgres_pwd)
    create_postgres_UScasualty_schema(table_name2,db_name,host_name,postgres_userid,postgres_pwd)
    insert_dfUSaccidents_into_postgres(df_mongo_crashes,db_name,table_name,host_name,postgres_userid,postgres_pwd)
    insert_dfUScasualty_into_postgres(df_mongo_people,db_name,table_name2,host_name,postgres_userid,postgres_pwd)
    
    
    #Read two tables from PostGRES SQL
    
    
    # Create an engine instance
    
    conn = psycopg2.connect(
       database="accidents_db", user=postgres_userid, password=postgres_pwd, host='127.0.0.1', port= '5432'
    )
    
    #Setting auto commit false
    conn.autocommit = True
    
    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()
    
    #Retrieving data
    cursor.execute('''SELECT * from accidents_us''')
    
    
    #Fetching 1st row from the table
    df_USaccident = cursor.fetchall();
    cols = list(map(lambda x: x[0], cursor.description))
    df_USaccident = pd.DataFrame.from_records(df_USaccident, columns=cols)
    df_USaccident.head()
    
    cursor.execute('''SELECT * from casualty_us''')
    
    
    #Fetching 1st row from the table
    df_UScasualty = cursor.fetchall();
    cols = list(map(lambda x: x[0], cursor.description))
    #df = DataFrame(data, columns=cols)
    df_UScasualty = pd.DataFrame.from_records(df_UScasualty, columns=cols)
    df_UScasualty.head()
    
    #Commit your changes in the database
    conn.commit()
    
    #Closing the connection
    conn.close()
    
    #Inner join two tables based on crash record ID to create a final data frame
    
    USaccident_finaldf = pd.merge(df_UScasualty,df_USaccident, on=['crash_record_id'], how='inner')
    USaccident_finaldf.head()
    USaccident_finaldf.dtypes
    USaccident_finaldf['year'] = pd.to_numeric(USaccident_finaldf['year'])
    USaccident_finaldf = USaccident_finaldf.drop_duplicates('crash_record_id', keep='first')
    
    
    #Visualisations - Year wise Accident numbers
    from matplotlib import pyplot as plt
    
    a=USaccident_finaldf.groupby(['year']).count()
    fig=plt.figure()
    fig.patch.set_facecolor('#DCDCDC')
    plt.plot(a['injuries_total'], color='#009DF1', label='Accidents',linewidth=2.5, marker='.', markersize=16, linestyle='--', markeredgecolor='black')
    plt.xlabel("Years", fontdict={'weight':'bold'})
    plt.ylabel("Total number of Accidents", fontdict={'weight':'bold'})
    plt.title('Year-wise Accident numbers', fontdict={'fontsize':15,'weight':'bold'})
    plt.grid(color = '#DCDCDC', linestyle = '--', linewidth = 1)
    plt.legend()
    plt.show()
    

# In[ ]:




