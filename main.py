import os
import json
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # <-- ADD THIS LINE
# import the error handling libraries for psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import pandas.io.sql as sqlio
import numpy as np

# import accidents_uk
# import Australia_code
# import ChicagoAccidentCode

# class accident_analysis:
#     accidents_uk.accidents_analysis_uk()
#     Australia_code.accidents_analysis_aus()
#     ChicagoAccidentCode.USAccidents()
    
f = open('db_credentials.json')
# returns JSON object as
# a dictionary
data = json.load(f)

postgres_userid = data["postgres_credentials"]["userid"]
postgres_pwd = data["postgres_credentials"]["password"]
postgres_host = data["postgres_credentials"]["host"]
postgres_port = data["postgres_credentials"]["port"]
postgres_db_name = data["postgres_credentials"]["dbname"]
f.close()

engine = create_engine('postgresql://'+postgres_userid+':'+postgres_pwd+'@'+postgres_host+':'+postgres_port+'/'+postgres_db_name,echo=False)


######Vishal Visualisation#####################################################

myqueryy = '''SELECT to_char(date_part('year', accidents_uk."Date"),'9999') AS Year, COUNT(accidents_uk."AccidentIndex") as totalaccidents
	 FROM accidents_uk inner join casualties_uk on accidents_uk."AccidentIndex"=casualties_uk."AccidentIndex" GROUP BY to_char(date_part('year', accidents_uk."Date"),'9999') ORDER BY to_char(date_part('year', accidents_uk."Date"),'9999') ASC;'''

df_yearwise_trends = pd.read_sql_query(myqueryy,con=engine)

Year = df_yearwise_trends["year"]
Unemployment_Rate = df_yearwise_trends["totalaccidents"]
  
plt.plot(Year, Unemployment_Rate, color='#009DF1', label='Accidents',linewidth=2.5, marker='.', markersize=16, linestyle='--', markeredgecolor='black')
plt.title('UK - Year wise Total number of accidents', fontdict={'fontsize':15,'weight':'bold'})
plt.xlabel('Year', fontdict={'weight':'bold'})
plt.ylabel('Total number of accidents', fontdict={'weight':'bold'})
plt.grid(color = '#DCDCDC', linestyle = '--', linewidth = 1)
plt.legend()
plt.show()


vishalquery = '''SELECT accidents_uk."Speedlimit" AS Speedlimit, COUNT(accidents_uk."AccidentIndex") as totalaccidents
	 FROM accidents_uk GROUP BY accidents_uk."Speedlimit" ORDER BY accidents_uk."Speedlimit" ASC;'''
# Writing Dataframe from sql query from postgresql
df_speed_limit = pd.read_sql_query(vishalquery,con=engine)

fig=plt.figure()
fig.patch.set_facecolor('#DCDCDC')
plt.plot(df_speed_limit['speedlimit'],df_speed_limit['totalaccidents'], color='#009DF1', label='Accidents',linewidth=2.5, marker='.', markersize=16, linestyle='--', markeredgecolor='black')
plt.xlabel("Speed Limit", fontdict={'weight':'bold'})
plt.ylabel("Total number of Accidents", fontdict={'weight':'bold'})
plt.title('UK - Number of Accidents based on Speed Limit', fontdict={'fontsize':15,'weight':'bold'})
plt.grid(color = '#DCDCDC', linestyle = '--', linewidth = 1)
plt.legend()
plt.show()

######End of Vishal Visualisation#####################################################

# ######Prashant Visualisation#####################################################


# USaccident_finaldf = ChicagoAccidentCode.USAccidents().USaccident_finaldf

# a=USaccident_finaldf.groupby(['year']).count()
# fig=plt.figure()
# fig.patch.set_facecolor('#DCDCDC')
# plt.plot(a['injuries_total'], color='#009DF1', label='Accidents',linewidth=2.5, marker='.', markersize=16, linestyle='--', markeredgecolor='black')
# plt.xlabel("Years", fontdict={'weight':'bold'})
# plt.ylabel("Total number of Accidents", fontdict={'weight':'bold'})
# plt.title('Year-wise Accident numbers', fontdict={'fontsize':15,'weight':'bold'})
# plt.grid(color = '#DCDCDC', linestyle = '--', linewidth = 1)
# plt.legend()
# plt.show()



# labels = ['Derbis','No defects','Others','Potholes','Wet road','Unknown','Worn Surface']
# sizes = USaccident_finaldf.groupby(['road_defect']).size()
# explode = (0, 0.2, 0, 0, 0, 0, 0)
# colors=['#87DFF1','#05839C','#87DFF1','#87DFF1','#87DFF1','#36AFC7','#87DFF1']
# fig1, ax1 = plt.subplots(figsize=(7,7))
# ax1.pie(sizes, explode=explode, colors=colors, labels=labels, autopct='%1.1f%%',shadow=False, startangle=90)
# ax1.axis('equal') # Equal aspect ratio ensures that pie is drawn as a circle.
# plt.title('Road Defects', fontdict={'fontsize':15,'weight':'bold'})
# plt.legend()
# plt.show()


# ######End of Prashant Visualisation#####################################################

# ######Tanmay Visualisation#####################################################

# Aust_final_df = Australia_code.accidents_analysis_aus().Aust_final_df

# #visualizations
# # Plotting Year graph to find out number of years and accident count
# a=Aust_final_df.groupby(['Year']).count()
# fig=plt.figure()
# fig.patch.set_facecolor('#DCDCDC')
# plt.plot(a['id'], color='#009DF1', label='Accidents',linewidth=2.5, marker='.', markersize=16, linestyle='--', markeredgecolor='black')
# plt.xlabel("Years", fontdict={'weight':'bold'})
# plt.ylabel("Total number of Accidents", fontdict={'weight':'bold'})
# plt.title('Year-wise Accident numbers', fontdict={'fontsize':15,'weight':'bold'})
# plt.grid(color = '#DCDCDC', linestyle = '--', linewidth = 1)
# plt.legend()
# plt.show()


# # Plotting State wise Pie chart to show accident percentage
# labels = ['Vic','NSW','Qld','WA','SA','NT','Tas','ACT']
# sizes = Aust_final_df.groupby(['State']).size()
# #explode = (0.0, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0)
# #colors=['#87DFF1','#05839C','#87DFF1','#87DFF1','#87DFF1','#36AFC7','#87DFF1']
# colors=['#17374F','#11293B','#1D4361','#2F6D98','#3A88C4','#62A0D0','#88B7DC']
# fig1, ax1 = plt.subplots(figsize=(10,10))
# ax1.pie(sizes, labels=labels, colors=colors,autopct='%1.1f%%',shadow=True, startangle=90)
# ax1.axis('equal') # Equal aspect ratio ensures that pie is drawn as a circle.
# plt.title('States', fontdict={'fontsize':15,'weight':'bold'})
# plt.legend()
# plt.show()

# ######End of Tanmay Visualisation#####################################################


# ##############################################################################


##Start of genderwise total number of accidents - UK
myquery1 = '''SELECT casualties_uk."SexofCasualty" AS Gender, COUNT(accidents_uk."AccidentIndex") as totalaccidents
	 FROM accidents_uk inner join casualties_uk on accidents_uk."AccidentIndex"=casualties_uk."AccidentIndex" GROUP BY casualties_uk."SexofCasualty" ORDER BY casualties_uk."SexofCasualty" ASC;'''
# Writing Dataframe from sql query from postgresql
df_genderwise_trends = pd.read_sql_query(myquery1,con=engine)


fig = plt.figure()
fig.patch.set_facecolor('#DCDCDC')
ax = fig.add_axes([0,0,1,1])
gender = df_genderwise_trends["gender"]
totalacc = df_genderwise_trends["totalaccidents"]
ax.bar(gender,totalacc,color=('#ffad33','#005bc2'),edgecolor=('black'))
plt.title('UK - Gender wise total number of accidents',fontdict={'fontsize':15,'weight':'bold'})
plt.xlabel('Gender', fontdict={'fontsize':13})
plt.ylabel('Total number of accidents', fontdict={'fontsize':13})
plt.show()


#End of genderwise total number of accidents


##############################################################################

    
##Start of genderwise total number of accidents - US
myquery2 = '''SELECT CASE WHEN casualty_us."sex" = 'F' THEN 'Female' WHEN casualty_us."sex" = 'M' THEN 'Male' END AS Gender, COUNT(accidents_us."crash_record_id") as totalaccidents
	 FROM accidents_us inner join casualty_us on accidents_us."crash_record_id"=casualty_us."crash_record_id" Where casualty_us."sex" <> 'X' GROUP BY casualty_us."sex" ORDER BY casualty_us."sex" ASC;'''
# Writing Dataframe from sql query from postgresql
df_genderwise_trends = pd.read_sql_query(myquery2,con=engine)


fig=plt.figure()
fig.patch.set_facecolor('#DCDCDC')
ax = fig.add_axes([0,0,1,1])
gender = df_genderwise_trends["gender"]
totalacc = df_genderwise_trends["totalaccidents"]
ax.bar(gender,totalacc,color=('#ffad33','#005bc2'),edgecolor=('black'))
plt.title('US - Gender wise total number of accidents',fontdict={'fontsize':15,'weight':'bold'})
plt.xlabel('Gender', fontdict={'fontsize':13})
plt.ylabel('Total number of accidents', fontdict={'fontsize':13})
plt.show()
##End of genderwise total number of accidents

##############################################################################

##Start of genderwise total number of accidents - Aus
myquery3 = '''SELECT australia_accidents."Gender" AS Gender, COUNT(australia_accidents."id") as totalaccidents
	 FROM australia_accidents WHERE australia_accidents."Gender" <> 'Unspecified' GROUP BY australia_accidents."Gender" ORDER BY australia_accidents."Gender" ASC;'''
# Writing Dataframe from sql query from postgresql
df_genderwise_trends = pd.read_sql_query(myquery3,con=engine)


fig=plt.figure()
fig.patch.set_facecolor('#DCDCDC')
ax = fig.add_axes([0,0,1,1])
gender = df_genderwise_trends["gender"]
totalacc = df_genderwise_trends["totalaccidents"]
ax.bar(gender,totalacc,color=('#ffad33','#005bc2'),edgecolor=('black'))
plt.title('Aus - Gender wise total number of accidents',fontdict={'fontsize':15,'weight':'bold'})
plt.xlabel('Gender', fontdict={'fontsize':13})
plt.ylabel('Total number of accidents', fontdict={'fontsize':13})
plt.show()
##End of genderwise total number of accidents


##############################################################################

##Start of day of week total number of accidents - UK
myquery4 = '''SELECT accidents_uk."DayofWeek",CASE WHEN accidents_uk."DayofWeek" ='Sunday' THEN '1'
WHEN accidents_uk."DayofWeek" ='Monday' THEN '2'
WHEN accidents_uk."DayofWeek" ='Tuesday' THEN '3'
WHEN accidents_uk."DayofWeek" ='Wednesday' THEN '4'
WHEN accidents_uk."DayofWeek" ='Thursday' THEN '5'
WHEN accidents_uk."DayofWeek" ='Friday' THEN '6'
WHEN accidents_uk."DayofWeek" ='Saturday' THEN '7'
END AS "DayofWeek_number", COUNT(accidents_uk."AccidentIndex") as totalaccidents
	 FROM accidents_uk GROUP BY accidents_uk."DayofWeek" ORDER BY "DayofWeek_number" DESC;'''

# Writing Dataframe from sql query from postgresql
df_dayofweek_trends = pd.read_sql_query(myquery4,con=engine)

fig = plt.figure()
fig=plt.figure()
fig.patch.set_facecolor('#DCDCDC')
dayofweek = df_dayofweek_trends["DayofWeek"]
totalacc = df_dayofweek_trends["totalaccidents"]
plt.barh(dayofweek,totalacc,color=('#0099cc'),edgecolor=('black'))
plt.title('UK - Weekcwise accident frequency',fontdict={'fontsize':15,'weight':'bold'})
plt.ylabel('Day of the week', fontdict={'fontsize':13})
plt.xlabel('Total number of accidents', fontdict={'fontsize':13})
plt.show()
##End of day of week total number of accidents - UK


##############################################################################

##Start of day of week total number of accidents - US
myquery5 = '''SELECT accidents_us."crash_day_of_week",CASE WHEN accidents_us."crash_day_of_week" ='1' THEN 'Sunday'
WHEN accidents_us."crash_day_of_week" ='2' THEN 'Monday'
WHEN accidents_us."crash_day_of_week" ='3' THEN 'Tuesday'
WHEN accidents_us."crash_day_of_week" ='4' THEN 'Wednesday'
WHEN accidents_us."crash_day_of_week" ='5' THEN 'Thursday'
WHEN accidents_us."crash_day_of_week" ='6' THEN 'Friday'
WHEN accidents_us."crash_day_of_week" ='7' THEN 'Saturday'
END AS "DayofWeek", COUNT(accidents_us."crash_record_id") as totalaccidents
	 FROM accidents_us GROUP BY accidents_us."crash_day_of_week" ORDER BY "crash_day_of_week" DESC;'''

# Writing Dataframe from sql query from postgresql
df_dayofweek_trends = pd.read_sql_query(myquery5,con=engine)


fig = plt.figure()
fig=plt.figure()
fig.patch.set_facecolor('#DCDCDC')
ax = fig.add_axes([0,0,1,1])
gender = df_dayofweek_trends["DayofWeek"]
totalacc = df_dayofweek_trends["totalaccidents"]
plt.barh(dayofweek,totalacc,color=('#0099cc'),edgecolor=('black'))
# ax.bar(gender,totalacc)
plt.title('US - Week wise accident frequency',fontdict={'fontsize':15,'weight':'bold'})
plt.ylabel('Day of the week', fontdict={'fontsize':13})
plt.xlabel('Total number of accidents', fontdict={'fontsize':13})
plt.show()
##End of day of week total number of accidents - US


##############################################################################

##Start of day of week total number of accidents - Aus
myquery5 = '''SELECT australia_accidents."Day",CASE WHEN australia_accidents."Day" ='Sunday' THEN '1'
WHEN australia_accidents."Day" ='Monday' THEN '2'
WHEN australia_accidents."Day" ='Tuesday' THEN '3'
WHEN australia_accidents."Day" ='Wednesday' THEN '4'
WHEN australia_accidents."Day" ='Thursday' THEN '5'
WHEN australia_accidents."Day" ='Friday' THEN '6'
WHEN australia_accidents."Day" ='Saturday' THEN '7'
END AS "DayofWeek_number", COUNT(australia_accidents."id") as totalaccidents
	 FROM australia_accidents GROUP BY australia_accidents."Day" ORDER BY "DayofWeek_number" DESC;'''

# Writing Dataframe from sql query from postgresql
df_dayofweek_trends = pd.read_sql_query(myquery5,con=engine)

fig=plt.figure()
fig.patch.set_facecolor('#DCDCDC')
ax = fig.add_axes([0,0,1,1])
dayofweek = df_dayofweek_trends["Day"]
totalacc = df_dayofweek_trends["totalaccidents"]
plt.barh(dayofweek,totalacc,color=('#0099cc'),edgecolor=('black'))
# ax.bar(gender,totalacc)
plt.title('Aus - Week wise accident frequency',fontdict={'fontsize':15,'weight':'bold'})
plt.ylabel('Day of the week', fontdict={'fontsize':13})
plt.xlabel('Total number of accidents', fontdict={'fontsize':13})
plt.show()
##End of day of week total number of accidents - Aus

##############################################################################

## Start Donut chart -Lighting conditions UK
myquery5 = '''SELECT A.LightConditions,COUNT(A.totalaccidents) AS totalaccidents
FROM( 
Select CASE WHEN accidents_uk."LightConditions" = 'Darkness  lights unlit' THEN 'Night'
WHEN accidents_uk."LightConditions" = 'Darkness  lights lit' THEN 'Night'
WHEN accidents_uk."LightConditions" = 'Darkness  no lighting' THEN 'Night'
WHEN accidents_uk."LightConditions" = 'Daylight' THEN 'Day' END AS LightConditions,"AccidentIndex" AS totalaccidents from accidents_uk)As A 
GROUP BY A.LightConditions ORDER BY A.LightConditions;'''

# Writing Dataframe from sql query from postgresql
df_lightcon_trends = pd.read_sql_query(myquery5,con=engine)

# Creating dataset
cars = df_lightcon_trends["lightconditions"]

data = df_lightcon_trends["totalaccidents"]

# Creating explode data
explode = (0.0, 0.0)

# Creating color parameters
colors = (  "orange", "cyan")

# Wedge properties
wp = { 'linewidth' : 1, 'edgecolor' : "black" }

# Creating autocpt arguments
def func(pct, allvalues):
    absolute = int(pct / 100.*np.sum(allvalues))
    return "{:.1f}%".format(pct, absolute)

# Creating plot
fig, ax = plt.subplots(figsize =(5, 5))
wedges, texts, autotexts = ax.pie(data,
autopct = lambda pct: func(pct, data),
explode = explode,
labels = cars,
shadow = False,
colors = colors,
startangle = 90,
wedgeprops = wp,
textprops = dict(color ="black"))

# Adding legend
ax.legend(wedges, cars,
title ="Light Conditions",
loc ="best",
bbox_to_anchor =(1, 0, 0.5, 1))

plt.setp(autotexts, size = 8, weight ="bold")
ax.set_title("UK - Accidents based on Light Conditions")

# show plot
plt.show()
## End Donut chart -Lighting conditions UK

##############################################################################

## Start Donut chart -Lighting conditions US
myquery5 = '''SELECT A.lighting_condition,COUNT(A.totalaccidents) AS totalaccidents
FROM( 
Select CASE WHEN accidents_us."lighting_condition" = 'DAWN' THEN 'Night'
WHEN accidents_us."lighting_condition" = 'DARKNESS' THEN 'Night'
WHEN accidents_us."lighting_condition" = 'DAYLIGHT' THEN 'Day'
WHEN accidents_us."lighting_condition" = 'DUSK' THEN 'Night'
WHEN accidents_us."lighting_condition" = 'DARKNESS, LIGHTED ROAD' THEN 'Night' END AS lighting_condition,"crash_record_id" AS totalaccidents from accidents_us WHERE accidents_us."lighting_condition" <> 'UNKNOWN')As A 
GROUP BY A.lighting_condition ORDER BY A.lighting_condition;'''

# Writing Dataframe from sql query from postgresql
df_lightcon_trends = pd.read_sql_query(myquery5,con=engine)

# Creating dataset
cars = df_lightcon_trends["lighting_condition"]

data = df_lightcon_trends["totalaccidents"]

# Creating explode data
explode = (0.0, 0.0)

# Creating color parameters
colors = (  "orange", "cyan")

# Wedge properties
wp = { 'linewidth' : 1, 'edgecolor' : "black" }

# Creating autocpt arguments
def func(pct, allvalues):
    absolute = int(pct / 100.*np.sum(allvalues))
    return "{:.1f}%".format(pct, absolute)

# Creating plot
fig, ax = plt.subplots(figsize =(5, 5))
wedges, texts, autotexts = ax.pie(data,
autopct = lambda pct: func(pct, data),
explode = explode,
labels = cars,
shadow = False,
colors = colors,
startangle = 90,
wedgeprops = wp,
textprops = dict(color ="black"))

# Adding legend
ax.legend(wedges, cars,
title ="Light Conditions",
loc ="best",
bbox_to_anchor =(1, 0, 0.5, 1))

plt.setp(autotexts, size = 8, weight ="bold")
ax.set_title("US - Accidents based on Light Conditions")

# show plot
plt.show()
## End Donut chart -Lighting conditions US

##############################################################################

## Start Donut chart -Lighting conditions Aus
myquery6 = '''Select australia_accidents."Time_of_day",COUNT("id") totalaccidents from australia_accidents GROUP BY australia_accidents."Time_of_day" ORDER BY australia_accidents."Time_of_day";'''

# Writing Dataframe from sql query from postgresql
df_lightcon_trends = pd.read_sql_query(myquery6,con=engine)

# Creating dataset
cars = df_lightcon_trends["Time_of_day"]

data = df_lightcon_trends["totalaccidents"]

# Creating explode data
explode = (0.0, 0.0)

# Creating color parameters
colors = ( "orange", "cyan")


# Wedge properties
wp = { 'linewidth' : 1, 'edgecolor' : "black" }


# Creating autocpt arguments
def func(pct, allvalues):
    absolute = int(pct / 100.*np.sum(allvalues))
    return "{:.1f}%".format(pct, absolute)

# Creating plot
fig, ax = plt.subplots(figsize =(5, 5))
wedges, texts, autotexts = ax.pie(data,
autopct = lambda pct: func(pct, data),
explode = explode,
labels = cars,
shadow = False,
colors = colors,
startangle = 90,
wedgeprops = wp,
textprops = dict(color ="black"))

# Adding legend
ax.legend(wedges, cars,
title ="Light Conditions",
loc ="best",
bbox_to_anchor =(1, 0, 0.5, 1))



plt.setp(autotexts, size = 8, weight ="bold")
ax.set_title("Aus - Accidents based on Light Conditions")

# show plot
plt.show()
## End Donut chart -Lighting conditions Aus


##############################################################################


