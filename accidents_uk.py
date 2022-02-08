import os
import json
import xmltodict
from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # <-- ADD THIS LINE
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors
#import psycopg2.extras as extras
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt
import pandas.io.sql as sqlio


class accidents_analysis_uk:

    def create_postgres_connection(db_name,host_name,port,user_name,pwd):
        conn = psycopg2.connect(
        database = db_name,
        user = user_name,
        password = pwd,
        host = host_name,
        port = port
        )
        return conn
    
    
    def get_df_from_postgres(db_name,host_name,port,user_name,pwd,sqlquery):
        engine = create_engine('postgresql://'+user_name+':'+pwd+'@'+host_name+':'+port+'/'+db_name,echo=False)
       
        # Writing Dataframe from sql query from postgresql
        return pd.read_sql_query(sqlquery,con=engine)
    
    def create_postgres_casualties_schema(table_name,db_name,host_name,user_name,pwd):
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
            cursor.execute("DROP TABLE IF EXISTS casualties_uk")
            
            #Drop existing database
            create_sql_query = '''CREATE TABLE ''' + table_name+''' (AccidentIndex VARCHAR(20),SexofCasualty VARCHAR(10), AgeofCasualty INT);'''
            print(create_sql_query)
            cursor.execute(create_sql_query)
        conn.close()
        return True
    
    
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
            
            #Droping table if already exists.
            cursor.execute("DROP TABLE IF EXISTS casualties_uk")
            
            #Droping table if already exists.
            cursor.execute("DROP TABLE IF EXISTS " + table_name)
            
            #Drop existing database
            #create_sql_query = '''CREATE TABLE accidents ( AccidentIndex VARCHAR(20) PRIMARY KEY, Longitude DECIMAL(9,6), Latitude DECIMAL(9,6), AccidentSeverity VARCHAR(20), NumberofVehicles INT, NumberofCasualties INT, DATE date, DayofWeek VARCHAR(20), Time VARCHAR(6),Speedlimit INT, LightConditions VARCHAR(50), WeatherConditions VARCHAR(50), RoadSurfaceConditions VARCHAR(50), UrbanorRuralArea VARCHAR(20) );'''
            create_sql_query = '''CREATE TABLE '''+ table_name +''' ( AccidentIndex VARCHAR(20) PRIMARY KEY, Longitude DECIMAL(9,6), Latitude DECIMAL(9,6), AccidentSeverity VARCHAR(20), NumberofVehicles INT, NumberofCasualties INT, DATE date, DayofWeek VARCHAR(20), Time VARCHAR(6),Speedlimit INT, LightConditions VARCHAR(50), WeatherConditions VARCHAR(50), RoadSurfaceConditions VARCHAR(50), UrbanorRuralArea VARCHAR(20) );'''
            cursor.execute(create_sql_query)
        conn.close()
    
    def insert_df_into_postgres(df,db_name,table_name,host_name,user_name,pwd):
        engine = create_engine('postgresql://'+user_name+':'+pwd+'@'+host_name+':5432/'+db_name,echo=False)
       
        # Writing Dataframe to PostgreSQL and replacing table if it already exists
        df.to_sql(name=table_name, con=engine, if_exists = 'replace', index=False)
    
    
    def clean_transform_casualtiesdf(dfcas):    
        dfcas = dfcas[dfcas["AgeofCasualty"]!="Data missing or out of range"]
        dfcas = dfcas[dfcas["AgeofCasualty"]!="#N/A"]
        dfcas = dfcas[dfcas["SexofCasualty"]!="Data missing or out of range"]
        dfcas = dfcas[dfcas["SexofCasualty"]!="#N/A"]
        
        dfcas["AgeofCasualty"] = pd.to_numeric(dfcas["AgeofCasualty"])
         
        # Selecting the relevant casualties columns
        dfcas=dfcas.drop(columns=["VehicleReference","CasualtyReference","CasualtyClass","AgeBandofCasualty","PedestrianLocation",
                                  "PedestrianMovement","CarPassenger","BusorCoachPassenger","PedestrianRoadMaintenanceWorker","CasualtyType",
                                  "CasualtyHomeAreaType","_id","@id"])
    
        return dfcas
    
    def clean_transform_accidentdf(dfacc):
        dfacc = dfacc[dfacc["Longitude"]!="Data missing or out of range"]
        dfacc["Date"] = dfacc["Date"].str.slice(0,8)
        
        dfacc["AccidentIndex"] = dfacc["AccidentIndex"]
        dfacc["Longitude"] = pd.to_numeric(dfacc["Longitude"])
        dfacc["Latitude"] = pd.to_numeric(dfacc["Latitude"])
        dfacc["AccidentSeverity"] = dfacc["AccidentSeverity"]
        dfacc["NumberofVehicles"] = pd.to_numeric(dfacc["NumberofVehicles"])
        dfacc["NumberofCasualties"] = pd.to_numeric(dfacc["NumberofCasualties"])
        dfacc["Date"] = pd.to_datetime(dfacc["Date"])
        dfacc["DayofWeek"] = dfacc["DayofWeek"]
        dfacc["Time"] = dfacc["Time"]
        dfacc["Speedlimit"] = pd.to_numeric(dfacc["Speedlimit"])
        dfacc["LightConditions"] = dfacc["LightConditions"]
        dfacc["WeatherConditions"] = dfacc["WeatherConditions"]
        dfacc["RoadSurfaceConditions"] = dfacc["RoadSurfaceConditions"]
        dfacc["UrbanorRuralArea"] = dfacc["UrbanorRuralArea"]
        
         
        #print (dfacc["AccidentIndex"])
        # Selecting the relevant accident columns
        dfacc=dfacc.drop(columns=["LocationEastingOSGR","LocationNorthingOSGR","PoliceForce","LocalAuthorityDistrict","LocalAuthorityHighway",
                                  "FirstRoadClass","FirstRoadNumber","RoadType","JunctionDetail","JunctionControl","SecondRoadClass",
                                  "SecondRoadNumber","PedestrianCrossingHumanControl","PedestrianCrossingPhysicalFacilities","SpecialConditionsatSite",
                                  "CarriagewayHazards","DidPoliceOfficerAttendSceneofAccident","LSOAofAccidentLocation","_id","@id"])
    
        return dfacc    
        
    
    ##Function to delete the existing accidents database from mongodb
    def check_mongodb(database_name,client):
        dbnames = client.list_database_names()
        if database_name in dbnames:
            client.drop_database(database_name)
        print("Existing DB "+database_name+" Database dropped successfully")
        return True
    
    ##Function to create and delete the existing accidents database from postgresql        
    def createpostgres_db(db_name,host_name,user_name,pwd):
        # connection establishment
        conn = psycopg2.connect(
        database = "postgres",
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
                
            if (db_name,) in list_database: #delete existing database
                #Terminate use of the database
                killdbuse_query = "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname ='" +db_name+"';"
                cursor.execute(killdbuse_query)
                print('Postgres existing db  killed !!!')
                
                #Drop existing database
                drop_sql_query = ''' DROP DATABASE '''+ db_name+''';''';
                cursor.execute(drop_sql_query)
                # executing above query
                print("Postgres sql Database " + db_name+" has been deleted successfully !!!");
             
      
            # query to create a database 
            create_db_sql_query = ''' CREATE DATABASE '''+ db_name+''';''';
            cursor.execute(create_db_sql_query)
            # executing above query
            print("Postgres sql Database " + db_name+" has been created successfully !!!");
        
        # Closing the connection
        conn.close()
        return True
    
    try:
        # Opening JSON file
        dbcredentials_file = 'db_credentials.json'
        f = open(dbcredentials_file)
        # returns JSON object as
        # a dictionary
        data = json.load(f)
        mongodb_userid = data["mongodb_credentials"]["userid"]
        mongodb_pwd = data["mongodb_credentials"]["password"]
        mongodb_host = data["mongodb_credentials"]["host"]
        mongodb_port = data["mongodb_credentials"]["port"]
        
        postgres_userid = data["postgres_credentials"]["userid"]
        postgres_pwd = data["postgres_credentials"]["password"]
        postgres_host = data["postgres_credentials"]["host"]
        postgres_port = data["postgres_credentials"]["port"]
        f.close()
    except IOError:
        print ("Could not read file: " + dbcredentials_file)
 
    #Initiate mongodb instance
    client = MongoClient("mongodb://%s:%s@127.0.0.1" % (mongodb_userid, mongodb_pwd))
    
    databasename = "accidents_db"
    accident_coll_name = "accidents_uk"
    casualties_coll_name = "casualties_uk"

    #check if accidents database in mongo db already exists
    check_mongodb(databasename,client)
    
    #create new database in mongo db
    db = client[databasename]
    
    #create collections : accidents, vehicles and casualties
    accidents_coll = db[accident_coll_name]
    casualties_coll = db[casualties_coll_name]
    
    try:
        #Read xml file, convert to dictionary and push to accident collection
        with open("accidents.xml") as xml_file1:
            acc_dict = xmltodict.parse(xml_file1.read())
            accidents_dict = acc_dict['data']['row']
            x = accidents_coll.insert_many(accidents_dict)
            xml_file1.close()
    except IOError:
        print ("Could not read file: accidents.xml")
    
    try:
        #Read xml file, convert to dictionary and push to casualties collection
        with open("casualties.xml") as xml_file3:
            cas_dict = xmltodict.parse(xml_file3.read())
            casualties_dict = cas_dict['data']['row']
            x = casualties_coll.insert_many(casualties_dict)
            xml_file3.close()
    except IOError:
        print ("Could not read file: casualties.xml")
        
    #Access accidents collection and convert to dataframe
    dfacc = db.accidents_uk
    dfacc = pd.DataFrame(list(dfacc.find()))
    
    #Clean and transform the accident dataframe
    dfacc_cleaned = clean_transform_accidentdf(dfacc)
    
    #Access casualties collection and convert to dataframe
    dfcas = db.casualties_uk
    dfcas = pd.DataFrame(list(dfcas.find()))
    
    #Clean and transform the casualties dataframe
    dfcas_cleaned = clean_transform_casualtiesdf(dfcas)
    
    #check if accident_uk postgres database already exists
    createpostgres_db(databasename,postgres_host,postgres_userid,postgres_pwd)
    
    #create accident table schema
    create_postgres_accidents_schema(accident_coll_name,databasename,postgres_host,postgres_userid,postgres_pwd)
    
    #insert accident data into respective table
    insert_df_into_postgres(dfacc_cleaned,databasename,accident_coll_name,postgres_host,postgres_userid,postgres_pwd)
    
    #create casualties table schema
    create_postgres_casualties_schema(casualties_coll_name,databasename,postgres_host,postgres_userid,postgres_pwd)
    
    #insert casualties data into respective table
    insert_df_into_postgres(dfcas_cleaned,databasename,casualties_coll_name,postgres_host,postgres_userid,postgres_pwd)
    
    
    myquery1 = '''SELECT casualties_uk."SexofCasualty", COUNT(accidents_uk."AccidentIndex") as totalaccidents
	 FROM accidents_uk inner join casualties_uk on accidents_uk."AccidentIndex"=casualties_uk."AccidentIndex" GROUP BY casualties_uk."SexofCasualty";'''
    
    df_acc = get_df_from_postgres(databasename,postgres_host,postgres_port,postgres_userid,postgres_pwd,myquery1)
    
    #print (df_acc.head())        
    #print (df_acc["SexofCasualty"]
    
    #ps_conn=create_postgres_connection(databasename,postgres_host,postgres_port,postgres_userid,postgres_pwd)
    #df_acc = sqlio.read_sql_query(myquery1, ps_conn)
    #print(df_acc)
    
    ##Start of Yearwise total number of accidents
    myquery2 = '''SELECT to_char(date_part('year', accidents_uk."Date"),'9999') AS Year, COUNT(accidents_uk."AccidentIndex") as totalaccidents
	 FROM accidents_uk inner join casualties_uk on accidents_uk."AccidentIndex"=casualties_uk."AccidentIndex" GROUP BY to_char(date_part('year', accidents_uk."Date"),'9999') ORDER BY to_char(date_part('year', accidents_uk."Date"),'9999') ASC;'''
    
    
    df_yearwise_trends = get_df_from_postgres(databasename,postgres_host,postgres_port,postgres_userid,postgres_pwd,myquery2)
    #print(df_yearwise_trends["year"])
    
    Year = df_yearwise_trends["year"]
    Unemployment_Rate = df_yearwise_trends["totalaccidents"]
  
    plt.plot(Year, Unemployment_Rate)
    plt.title('Yearwise Total number of accidents')
    plt.xlabel('Year')
    plt.ylabel('Total number of accidents')
    plt.show()
    
    ##End of Yearwise total number of accidents
    
    ##Start of genderwise total number of accidents
    myquery3 = '''SELECT to_char(date_part('year', accidents_uk."Date"),'9999') AS Year,casualties_uk."SexofCasualty" AS Gender, COUNT(accidents_uk."AccidentIndex") as totalaccidents
	 FROM accidents_uk inner join casualties_uk on accidents_uk."AccidentIndex"=casualties_uk."AccidentIndex" GROUP BY to_char(date_part('year', accidents_uk."Date"),'9999') ,casualties_uk."SexofCasualty" ORDER BY to_char(date_part('year', accidents_uk."Date"),'9999'),casualties_uk."SexofCasualty" ASC;'''
    
    
    df_genderwise_trends = get_df_from_postgres(databasename,postgres_host,postgres_port,postgres_userid,postgres_pwd,myquery3)
    #print(df_genderwise_trends)
    #print(df_genderwise_trends[df_genderwise_trends["gender"]=="Female"])
    df_female = df_genderwise_trends[df_genderwise_trends["gender"]=="Female"]
    df_male = df_genderwise_trends[df_genderwise_trends["gender"]=="Male"]
    
    df1=df_female.merge(df_male, on=('year'))
    df1=df1.rename(columns={"totalaccidents_x": "Female","totalaccidents_y":"Male"})
    df1.plot(kind='bar', y=["Female","Male"], x="year")
    plt.show()
    
    ##End of genderwise total number of accidents
    
    
    
    
    

    

