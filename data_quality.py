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


# DB 폴더 안에 있는 것 밀어 넣기
# for filename in txtfiles :

#     df = pd.read_csv(f"./{filename}")
#     table_name = filename[:-4]            #.csv 지우고 db에 넣기

#     host = "127.0.0.1"
#     user = "root"
#     password = "0000"
#     port = "3306"
#     database = "csv_db4"                  #넣을 db이름
#     engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', encoding='utf-8')

#     df.to_sql(index = False,
#             name = table_name,
#             con = engine,
#             if_exists = 'append',
#             method = 'multi',
#             chunksize = 10000)

# print("finish!!!")



# Connect to the database
conn = pymysql.connect(host="127.0.0.1", user="root", password="0000", db="csv_db4")

# Create a cursor object
cursor = conn.cursor()

# 쿼리 날리기

# # Define your query as a string
# query1 = "set @row_num := 0;"
# query2 = "create table choi_test3 select @row_num := @row_num +1 as abst_row_num__ , a.* from example2 as a ;"

# # Execute the query
# cursor.execute(query1)
# cursor.execute(query2)

# # Commit the changes
# conn.commit()

# # Close the cursor and connection
# cursor.close()
# conn.close()


# Get the total number of records in the table
query = "SELECT COUNT(*) FROM example2"
cursor.execute(query)
total_records = cursor.fetchone()[0]
print(total_records)


chunk = 20
# 분할 처리
# Iterate through the records in steps of 20
for i in range(1, total_records+1, chunk):
    try:
        query = f"SELECT * FROM example2 WHERE record_number BETWEEN {i} AND {i + chunk - 1}"
        cursor.execute(query)
        result = cursor.fetchall()

        # running over index
        if i + chunk >= total_records:
            query = f"SELECT * FROM example2 WHERE record_number BETWEEN {i} AND {total_records}"
            cursor.execute(query)
            result = cursor.fetchall()
            break
    except Exception as e:
        print()
        print("---------------------------------------------")
        print("Data_Insert :", e)
        print("---------------------------------------------")
        print()
        # process the result here
        break
# Commit the changes
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()


# 데이터가 고정된 경우 이 코드가 유리하지만 계속 변경되는 데이터에 대해서 처리해야 하기 때문에 위 코드가 적절하다
# offset = 0
# while True:
#     query = f"SELECT * FROM table_name LIMIT 20 OFFSET {offset}"
#     cursor.execute(query)
#     result = cursor.fetchall()
#     if not result:
#         break
#     # process the result here
#     offset += 20


# 예약어가 컬럼명에 들어가있는지 확인하는 쿼리
import mysql.connector

# Connect to the database
cnx = mysql.connector.connect(user="root", password="0000",
                              host="127.0.0.1",
                              database="csv_db4")

# Create a cursor object
cursor = cnx.cursor()

# Define the list of reserved words
reserved_words = ['SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'ORDER', 'BY']
column_patterns = ['"', "'", '{', '}']
space_ = [' ']


# Define the table name
table_name = 'choi_test2'

print()
print("---------------------------------------------")
print("reserved word exist: ")

for reserved_word in reserved_words:
    # Execute a query to get all column names from the table
    # OR 을 찾는 경우 theory, SELECT를 찾는 경우 SELECTION 같은 단어도 검출 되기 때문에, 데이터 잔존률을 유지하기 위해 SELECT, OR 같이 예약어와 정확히 일치하는 경우만 검출
    # 정확히 일치하는 단어는 하나밖에 없기 때문에 여러 개가 존재할 수 없음
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND LOWER(column_name) = '{reserved_word}'"
    cursor.execute(query)
    column_names = cursor.fetchall()
    print(column_names)

print()
print("---------------------------------------------")
print("special character exist: ")
for column_pattern in column_patterns:
    # Execute a query to get all column names from the table
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND LOWER(column_name) LIKE '%\{column_pattern}%'"
    cursor.execute(query)
    column_names = cursor.fetchall()
    print(column_names)
exit()


# Use a for loop to check if any column names contain any of the reserved words
for column_name in column_names:
    for reserved_word in reserved_words:
        if reserved_word in column_name[0]:
            print(f"The column name {column_name[0]} contains the reserved word {reserved_word}.")

# Close the cursor and connection
cursor.close()
cnx.close()
