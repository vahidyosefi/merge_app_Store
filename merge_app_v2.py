# -*- coding: utf-8 -*-
"""
Created on Wed Apr 27 11:00:38 2022

@author: Vahid
for merge postgres for APP
"""



import pandas as pd
import numpy as np
import datetime
import os
import time
import pyodbc
from pyodbc import *
import psycopg2
import pandas.io.sql as psql
from sqlalchemy import create_engine


# start = time.time() 

# df0 = pd.DataFrame()
# total = pd.DataFrame()


######################## read data from dabase
connection = psycopg2.connect(user="postgres",
                            password="12344321",
                            host="10.32.141.17",
                            port="5432",
                            database="googleplay1")
cursor = connection.cursor()
#########   googleplay
googleplay = psql.read_sql('select * from public."googleplay_meta"', connection)
print(len(googleplay))


googleplay['cat'] = googleplay['cat'].str.replace('gane','game')
googleplay['platform'] = 'googleplay'
googleplay['creator'] = ''
googleplay['OS'] = 'Android'
googleplay['age range']=''
googleplay['Cost']=''
googleplay['Updated']=googleplay['Last Update']
googleplay['Installs']=googleplay['Download']

googleplay = googleplay[['cat','categori_name','content_name','content_link','rate','ratings','Updated','Size','Installs','Current Version','crawling_date','age range','Cost','creator','OS','platform','Device cat']]

# googleplay.to_excel(r'D:\python\PowerBI_App_store\progress\test\googleplay1.xlsx', index=False)

googleplay.dtypes


print('googleplay:',len(googleplay))
print('فراخونی محتوای googleplay')


############## cafebazar


connection = psycopg2.connect(user="postgres",
                            password="12344321",
                            host="10.32.141.17",
                            port="5432",
                            database="cafebazar")
cursor = connection.cursor()


cafebazar = psql.read_sql('select * from public."cafebazar_meta"', connection)

cafebazar.dtypes
print("cafebazar:" ,len(cafebazar))

print('فراخونی محتوای cafebazar')

cafebazar['platform'] = 'cafebazar'
cafebazar['OS'] = 'Android'
cafebazar['creator'] = ''
cafebazar['Updated'] = ''
cafebazar['Device cat'] = ''

cafebazar['age range']=''
cafebazar['Cost']=''

cafebazar = cafebazar[['cat','categori_name','content_name','content_link','rate','ratings','Updated','Size','Installs','Current Version','crawling_date','age range','Cost','creator','OS','platform','Device cat']]

# cafebazar.to_excel(r'D:\python\PowerBI_App_store\progress\test\cafebazar00.xlsx', index=False)


#####################################################

###########  myket

connection = psycopg2.connect(user="postgres",
                            password="12344321",
                            host="10.32.141.17",
                            port="5432",
                            database="Myket")
cursor = connection.cursor()


myket = psql.read_sql('select * from public."myket_meta"', connection)
print('myket:',len(myket))
print('فراخونی محتوای myket')
# gabeh_1000 = gabeh[1:1000]
myket['platform'] = 'myket'
myket['OS'] = 'Android'
myket['creator'] = myket['Creator']
myket['Installs'] = myket['Download']
myket['Updated'] = myket['Last Update']

myket['age range']=''
myket['Cost']=''
myket['Device cat']=''

myket.dtypes

myket = myket[['cat','categori_name','content_name','content_link','rate','ratings','Updated','Size','Installs','Current Version','crawling_date','age range','Cost','creator','OS','platform','Device cat']]


# myket.to_excel(r'D:\python\PowerBI_App_store\progress\test\myket1.xlsx', index=False)


####################################################


############       anardoni

connection = psycopg2.connect(user="postgres",
                            password="12344321",
                            host="10.32.141.17",
                            port="5432",
                            database="anardoni")
cursor = connection.cursor()

anardoni = psql.read_sql('select * from public."anardoni_meta"', connection)
print('anardoni:',len(anardoni))
print('فراخونی محتوای anardoni')

anardoni['Installs'] = anardoni['Download']
anardoni['Updated'] = anardoni['Last Update']
anardoni['creator'] = anardoni['Creator']

anardoni['platform'] = 'anardoni'
anardoni['OS'] = 'IOS'

anardoni['Device cat']=''
# anardoni['Cost']=

anardoni['rate'] = anardoni['rate'].astype(str).astype(float)
anardoni['ratings'] = anardoni['ratings'].astype(str).astype(int)
anardoni['Installs'] = anardoni['Installs'].astype(str).astype(int)

anardoni = anardoni[['cat','categori_name','content_name','content_link','rate','ratings','Updated','Size','Installs','Current Version','crawling_date','age range','Cost','creator','OS','platform','Device cat']]


# anardoni.to_excel(r'D:\python\PowerBI_App_store\progress\test\anardoni1.xlsx', index=False)
anardoni.dtypes


################################################


################   nassaab

connection = psycopg2.connect(user="postgres",
                            password="12344321",
                            host="10.32.141.17",
                            port="5432",
                            database="nassaab")
cursor = connection.cursor()

nassaab = psql.read_sql('select * from public."nassaab_meta"', connection)
print('nassaab:',len(nassaab))
print('فراخونی محتوای nassaab')
nassaab['platform'] = 'nassaab'

nassaab['Installs'] = nassaab['Download']
nassaab['Updated'] = nassaab['Last Update']
nassaab['creator'] = nassaab['Creator']
nassaab['age range']=''
nassaab['Cost']=''
nassaab['OS'] = 'IOS'
nassaab['Device cat']=''

nassaab.dtypes

nassaab['Installs'] = nassaab['Installs'].astype(str).astype(int)

nassaab = nassaab[['cat','categori_name','content_name','content_link','rate','ratings','Updated','Size','Installs','Current Version','crawling_date','age range','Cost','creator','OS','platform','Device cat']]
# nassaab.to_excel(r'D:\python\PowerBI_App_store\progress\test\nassaab1.xlsx', index=False)

print('nassaab:', len(nassaab))



######################################################

# ################   sibapp


connection = psycopg2.connect(user="postgres",
                            password="12344321",
                            host="10.32.141.17",
                            port="5432",
                            database="sibapp")
cursor = connection.cursor()

sibapp = psql.read_sql('select * from public."sibapp_meta"', connection)
print('sibapp :',len(sibapp))
print('فراخونی محتوای sibapp')


sibapp['platform'] = 'sibapp'

sibapp['Installs'] = sibapp['Download']
sibapp['Updated'] = ''
sibapp['creator'] = ''
sibapp['age range']=''
sibapp['Cost']=''
sibapp['OS'] = 'IOS'
sibapp['Device cat'] = ''
sibapp.dtypes

sibapp = sibapp[['cat','categori_name','content_name','content_link','rate','ratings','Updated','Size','Installs','Current Version','crawling_date','age range','Cost','creator','OS','platform','Device cat']]
# sibapp.to_excel(r'D:\python\PowerBI_App_store\progress\test\sibapp.xlsx', index=False)




# #########################################################

# ################  sibche


connection = psycopg2.connect(user="postgres",
                            password="12344321",
                            host="10.32.141.17",
                            port="5432",
                            database="sibche")
cursor = connection.cursor()

sibche = psql.read_sql('select * from public."sibche_meta"', connection)
print('sibche:',len(sibche))
print('فراخونی محتوای sibche')

sibche['platform'] = 'sibche'
sibche['Updated'] = ''
sibche['OS'] = 'IOS'
sibche['Device cat'] = ''
sibche.dtypes

# sibche['age range']=''
sibche['Cost']=''

sibche = sibche[['cat','categori_name','content_name','content_link','rate','ratings','Updated','Size','Installs','Current Version','crawling_date','age range','Cost','creator','OS','platform','Device cat']]



#############################################################

###############  merge

app_merge = [googleplay,cafebazar,myket,anardoni,nassaab,sibapp,sibche]
adgham_app = pd.concat(app_merge)


adgham_app['rate'] = adgham_app['rate'].astype(str).astype(float)
adgham_app['ratings'] = adgham_app['ratings'].astype(str).astype(int)
adgham_app['Installs'] = adgham_app['Installs'].astype(int)



adgham_app.dtypes

sibche = sibche[['cat','categori_name','content_name','content_link','rate','ratings','Updated','Size','Installs','Current Version','crawling_date','age range','Cost','creator','OS','platform','Device cat']]


# adgham0['viwes'] = adgham0['viwes'].astype(float)
# adgham0['like_count'] = adgham0['like_count'].astype(float)
# adgham0['follower'] = adgham0['follower'].astype(float)

engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Vahid01',pool_size=20, max_overflow=100,)
con=engine.connect()


adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Action','اکشن')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Sports','ورزشی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Social','اجتماعی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Puzzle','معما')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Communication','ارتباطی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Art & Design','هنر و  طراحی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Watch apps','برنامه های ساعت هوشمند')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Productivity','بهره وری')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Photography','عکاسی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Video Players & Editors','ویرایش و پخش فیلم')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Casual','تفننی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('شبیه‌سازی','شبیه سازی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('کلمات و دانستنی‌ها','کلمات و دانستنی ها')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Simulation','شبیه سازی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Adventure','ماجراجویی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Arcade','آرکید')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Board','تخته‌ای')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Racing','ورزشی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Educational','آموزشی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Role Playing','نقش آفرینی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Strategy','استراتژی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Word','کلمات و دانستنی ها')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Card','کارت بازی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Casino','کازینو')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Music','موسیقی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Education','آموزشی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('آموزش','آموزشی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Medical','پزشکی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Lifestyle','سبک زندگی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('کتب','کتاب')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Health & Fitness','سلامتی و تناسب اندام')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('Graphics & Design','هنر و  طراحی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('شخصی‌سازی','شخصی سازی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('شبکه های اجتماعی','شبکه‌های اجتماعی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('شبکه اجتماعی','شبکه‌های اجتماعی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('اجتماعی Networking','شبکه‌های اجتماعی')
adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('آموزشیی','آموزشی')
# adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('','')
# adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('','')
# adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('','')
# adgham_app['categori_name'] = adgham_app['categori_name'].str.replace('','')


# adgham_app.to_excel(r'D:\python\PowerBI_App_store\progress\test\adgham_app4.xlsx', index=False)
print('adgham_app :',len(adgham_app))
adgham_app.to_sql('App_merge',con,if_exists='replace', index=False)

print('اتمام برنامه')

adgham_app.dtypes

creator = adgham_app.query(" creator != ''")
print('just_creator_v1 :',len(creator))
creator.to_excel(r'D:\python\PowerBI_App_store\progress\test\creator.xlsx', index=False)
creator.to_sql('creator_App_merge',con,if_exists='replace', index=False)



cat = adgham_app.query(" cat == 'game'")
print('just_cat :',len(cat))
cat.to_excel(r'D:\python\PowerBI_App_store\progress\test\cat.xlsx', index=False)
cat.to_sql('game_App_merge',con,if_exists='replace', index=False)




learn1 = adgham_app.query("categori_name == 'آموزشی'")
learn2 = adgham_app.query("categori_name == 'هنر و  طراحی'")
learn3 = adgham_app.query("categori_name == 'سبک زندگی'")
learn4 = adgham_app.query("categori_name == 'سلامتی و تناسب اندام'")
learn5 = adgham_app.query("categori_name == 'پزشکی'")
learn6 = adgham_app.query("categori_name == 'تناسب اندام'")
learn7 = adgham_app.query("categori_name == 'کتاب‌ها و مطبوعات'")
learn8 = adgham_app.query("categori_name == 'پزشکی و سلامت'")
learn9 = adgham_app.query("categori_name == 'کسب و کار'")
learn10 = adgham_app.query("categori_name == 'کتاب'")
# learn = adgham_app.query("categori_name == 'آموزشی'")
# learn = adgham_app.query("categori_name == 'آموزشی'")
# learn = adgham_app.query("categori_name == 'آموزشی'")

learn0 = [learn1, learn2, learn3, learn4, learn5, learn6, learn7, learn8, learn9, learn10]
learn = pd.concat(learn0)

print('learn :',len(learn))
learn.to_excel(r'D:\python\PowerBI_App_store\progress\test\learn2.xlsx', index=False)
learn.to_sql('learn_app',con,if_exists='replace', index=False)


