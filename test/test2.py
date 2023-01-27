import pandas as pd
import mariadb
import pymysql
from sqlalchemy import create_engine
import os
import re

# arr = os.listdir()
# print(arr)

import glob

txtfiles = []
for file in glob.glob("*.csv"):
    txtfiles.append(file)

print(txtfiles)

# a = '222.csv'
# print(a[:-4])

# print("111")
# exit()

# for i in range(len(txtfiles)):
#     table_name = f"{txtfiles[i]}"
    
for i in txtfiles :
    
    # df = pd.read_csv("C:/Users/Yonsei/Downloads/소상공인시장진흥공단_상가(상권)정보_20220930/soho_seoul.csv")
    df = pd.read_csv(f"./{i}")
    
    # 여기서 속성명 속성값 검사
    print(df.columns)
    
    
    
    
    table_name = 'test' + i[:-4]
    
    host = "127.0.0.1"
    user = "root"
    password = "0000"
    port = "3306"
    database = "csv_db"

    # 위 커넥션 정보와 동일하게 입력
    # engine = create_engine("mysql://{user}:{pw}@localhost/{db}".format(user='root', pw='0000', db='csv_db3'))
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', encoding='utf-8')

    df.to_sql(index = False,
            name = table_name,
            con = engine,
            if_exists = 'append',
            method = 'multi', 
            chunksize = 10000)

print("finish!!!")
exit()
# data = list(df.itertuples(index=False, name=None))
# print(data[0])

