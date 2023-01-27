import pandas as pd
import mariadb
import pymysql
from sqlalchemy import create_engine
import glob

txtfiles = []
for file in glob.glob("*.csv"):
    txtfiles.append(file)

print(txtfiles)

# print("111")
# exit()
# for i in range(len(txtfiles)):
#     table_name = f"{txtfiles[i]}"

for filename in txtfiles :

    df = pd.read_csv(f"./{filename}")
    table_name = filename[:-4]            #.csv 지우고 db에 넣기

    host = "127.0.0.1"
    user = "root"
    password = "0000"
    port = "3306"
    database = "csv_db5"                  #넣을 db이름
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', encoding='utf-8')

    df.to_sql(index = False,
            name = table_name,
            con = engine,
            if_exists = 'append',
            method = 'multi',
            chunksize = 10000)

print("finish!!!")
exit()