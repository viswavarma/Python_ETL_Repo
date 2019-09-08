import pymysql as db
from pandas import DataFrame
import pandas as pd
import numpy as np
import time
import datetime
cur_length=0
sql=""" create table Statistics(
Column_Name varchar(255),
Average float,
Standard_Deviation float,
Median float,
Count int,
timestamp varchar(255)
)"""
i=1
while True:

    conn1=db.connect(host="xxxxxxxxxxxxxxx", port=3306,user='xxx',passwd='xxxxx',db='ApplicationData')
    conn2=db.connect(host="xxxxx", port=3306,user='xxxxx',passwd='xxxx,db='xxxx')
    cur1=conn1.cursor()
    cur2=conn2.cursor()
    if i==1:
        cur2.execute("drop table if exists Statistics")
        cur2.execute(sql)
        i = 0

    cur1.execute("select lot_size_sqft, total_building_sqft, yr_built, bedrooms, total_rooms, bath_total, final_value from raw_data")
    #Convered resultset into dataframe
    df=DataFrame(list(cur1.fetchall()),
                 columns=['lot_size_sqft', 'total_building_sqft', 'yr_built', 'bedrooms', 'total_rooms', 'bath_total', 'final_value'],index=None)
    columns = list(df.columns.values)
    new_length=df.shape[0]
    print(new_length)
    if new_length>cur_length:
        for col in columns:
            col_name = col
            df[col] = df[col].convert_objects(convert_numeric = True)
            avg = (df[col]).replace(0,np.nan).dropna().mean()
            sd = (df[col]).replace(0,np.nan).dropna().std()
            #print type(sd.astype(float))
            median = (df[col]).replace(0,np.nan).dropna().median()
            count=(df[col]).replace(0,np.nan).dropna().count()
            date=datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            cur2.execute("insert into Statistics values ('%s', '%f', '%f', '%f', '%d', '%s')" %
                            (col_name,avg,sd,median,count,date))
            cur_length=new_length
            conn2.commit()
    
    time.sleep(120)


#developed 