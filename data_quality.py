import pandas as pd
import mariadb
import pymysql
from sqlalchemy import create_engine
import glob
import re
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


# chunk = 20
# # 분할 처리
# # Iterate through the records in steps of 20
# for i in range(1, total_records+1, chunk):
#     try:
#         query = f"SELECT * FROM example2 WHERE record_number BETWEEN {i} AND {i + chunk - 1}"
#         cursor.execute(query)
#         result = cursor.fetchall()
#         # running over index
#         if i + chunk >= total_records:
#             query = f"SELECT * FROM example2 WHERE record_number BETWEEN {i} AND {total_records}"
#             cursor.execute(query)
#             result = cursor.fetchall()
#             break
#     except Exception as e:
#         print()
#         print("---------------------------------------------")
#         print("Data_Insert :", e)
#         print("---------------------------------------------")
#         print()
#         # process the result here
#         break
# # Commit the changes
# conn.commit()

# # Close the cursor and connection
# cursor.close()
# conn.close()





print('--------------------------------------')
print('속성값 변경 함수 : ')
print()


def change_word_null(df, patterns):   #  dataframe 안의 값 중 patterns 에 해당하는 단어들을 ''로 바꿔주는 함수
    # Create the regular expression pattern
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)
    # Use the applymap() method to apply the re.sub() function to each element in the DataFrame
    result = df.applymap(lambda x: re.sub(pattern, '', x) if isinstance(x, str) else x)
    return result

# db info
host = "127.0.0.1"
user = "root"
password = "0000"
port = "3306"
database = "csv_db4"
table_name = 'choi_test2'

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', encoding='utf-8')
chunk = 20
null_words = ['\\\\n', 'N/A']  # ---> ''으로 변경

def change_value_word(table_name, null_words, engine, chunk):
    # get total record
    conn = engine.connect()
    query = "SELECT COUNT(*) FROM example2"
    cursor.execute(query)
    total_records = cursor.fetchone()[0]
    print(total_records)

    for i in range(1, total_records+1, chunk):
        try:
            query = f"SELECT * FROM {table_name} WHERE abst_row_num__ BETWEEN {i} AND {i + chunk - 1}"
            cursor.execute(query)
            result = cursor.fetchall()
            df = pd.DataFrame(result)
            df = change_word_null(df, null_words)
            df.to_sql(index = False, name = 'modified_table5', con = engine, if_exists = 'append', method = 'multi', chunksize = chunk)
            # print(df)

            # running over index
            if i + chunk >= total_records:
                query = f"SELECT * FROM {table_name} WHERE abst_row_num__ BETWEEN {i} AND {total_records}"
                cursor.execute(query)
                result = cursor.fetchall()
                df = pd.DataFrame(result)
                df = change_word_null(df, null_words)
                df.to_sql(index = False, name = 'modified_table5', con = engine, if_exists = 'append', method = 'multi', chunksize = chunk)
                # print(df)
                break
        except Exception as e:
            print()
            print("---------------------------------------------")
            print("Data_Insert :", e)
            print("---------------------------------------------")
            print()
            # process the result here
            break
    # Close the cursor and connection
    cursor.close()
    conn.close()

change_value_word(table_name, null_words, engine, chunk)
print('finish!!')


# 4. 속성 유형 불러오기
connection = engine.connect()

# 불러올 테이블명
table_name = 'modified_table'

query = f"SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME = '{table_name}'"
col_types = connection.execute(query).fetchall()
# close the connection
# connection.close()

clear_col_type = []
for col_type in col_types:
    clear_col_type.append(col_type[0])


# 5. 범주형 속성이 존재하는지
categorical_type = ('enum', 'set', 'char', 'varchar', 'text', 'tinytext', 'mediumtext', 'longtext', 'json')

categorical_col_type = []
for col_type in clear_col_type:
    if col_type in categorical_type:
        categorical_col_type.append(True)       #범주형
    else:  categorical_col_type.append(False)   #수치형

print(categorical_col_type)

# 6. 수치형 속성의 값이 모두 동일하거나 NULL인지 확인
# Define the table name
db_name = 'csv_db4'
table_name = 'choi_test'

# db table column name 가져와서 clear_col_name 이라는 list에 넣는 코드
query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}'"
column_names = connection.execute(query).fetchall()
clear_col_name = []
for col in column_names:
    clear_col_name.append(col[0])

# query = f"SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME = '{table_name}'"
# col_types = connection.execute(query).fetchall()



print(clear_col_name)
# false 인 열들을 조사
for i, col_type in enumerate(categorical_col_type):
    # print(i,col_type)
    if col_type == False:
        query = f"SELECT COUNT(DISTINCT {clear_col_name[i]}) FROM {table_name}"
        unique_num = connection.execute(query).fetchone()
        print(unique_num[0])
    else: continue
# close the connection
connection.close()
exit()

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
column_patterns = ['\\\\n', "N/A", '{', '}']
masq_commands = ['abst_row_num__', 'rec_disim', 'rec_numerical', 'rec_categorical']
space_ = [' ']


# Define the table name
db_name = 'csv_db4'
table_name = 'choi_test2'


# print()
# print("---------------------------------------------")
# print("reserved word exist: ")
# def col_reserved_word_check(reserved_words):
#     col_lst = []
#     for reserved_word in reserved_words:
#         # Execute a query to get all column names from the table
#         # OR 을 찾는 경우 theory, SELECT를 찾는 경우 SELECTION 같은 단어도 검출 되기 때문에, 데이터 잔존률을 유지하기 위해 SELECT, OR 같이 예약어와 정확히 일치하는 경우만 검출
#         # 정확히 일치하는 단어는 하나밖에 없기 때문에 여러 개가 존재할 수 없음
#         # print(column_patern)

#         query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}' AND LOWER(column_name) LIKE '{reserved_word}'"
#         cursor.execute(query)
#         column_names = cursor.fetchall()
#         # print(column_names)
#         col_lst.append(column_names)
#     return col_lst

# print(col_reserved_word_check(reserved_words))
# exit()

# print()
# print("---------------------------------------------")
# print("masq command exist: ")
# def col_masq_command_check(masq_commands):
#     col_lst = []
#     for masq_command in masq_commands:
#         # Execute a query to get all column names from the table
#         # OR 을 찾는 경우 theory, SELECT를 찾는 경우 SELECTION 같은 단어도 검출 되기 때문에, 데이터 잔존률을 유지하기 위해 SELECT, OR 같이 예약어와 정확히 일치하는 경우만 검출
#         # 정확히 일치하는 단어는 하나밖에 없기 때문에 여러 개가 존재할 수 없음
#         # print(column_patern)

#         query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}' AND LOWER(column_name) LIKE '{masq_command}'"
#         cursor.execute(query)
#         column_names = cursor.fetchall()
#         # print(column_names)
#         col_lst.append(column_names)
#     return col_lst

# print(col_masq_command_check(masq_commands))
# exit()

# print()
# print("---------------------------------------------")
# print("special character exist: ")
# def col_special_char_check(column_patterns):
#     col_lst = []
#     for column_patern in column_patterns:
#         # Execute a query to get all column names from the table
#         # OR 을 찾는 경우 theory, SELECT를 찾는 경우 SELECTION 같은 단어도 검출 되기 때문에, 데이터 잔존률을 유지하기 위해 SELECT, OR 같이 예약어와 정확히 일치하는 경우만 검출
#         # 정확히 일치하는 단어는 하나밖에 없기 때문에 여러 개가 존재할 수 없음
#         # print(column_patern)
#         if column_patern == '\\\\n':     # \n인 경우만 따로 검사
#             query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}' AND LOWER(column_name) = '{column_patern}'"
#             cursor.execute(query)
#             column_names = cursor.fetchall()
#             col_lst.append(column_names)
#             # print(column_names)
#             continue
#         query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}' AND LOWER(column_name) LIKE '%{column_patern}%'"
#         cursor.execute(query)
#         column_names = cursor.fetchall()
#         col_lst.append(column_names)
#         # print(column_names)
#     return col_lst

# print(col_special_char_check(column_patterns))

# exit()

# print()
# print("---------------------------------------------")
# print("special character exist: ")


# # 특수문자 바꾸는 함수

# # db table column name 가져와서 clear_col_name 이라는 list에 넣는 코드
# query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}'"
# cursor.execute(query)
# column_names = cursor.fetchall()
# clear_col_name = []
# for col in column_names:
#     clear_col_name.append(col[0])


# def change_word(lst, patterns):
#     pattern = '|'.join(patterns)
#     pattern = re.compile(pattern, flags=re.IGNORECASE)
#     result = [re.sub(pattern, '_', x) if isinstance(x, str) else x for x in lst]
#     return result

# print(change_word(clear_col_name, column_patterns))
# print(clear_col_name)

# # print(clear_col_name)
# exit()


# 데이터 속성값에 특수문자가 들어갔는지 확인



print()
print("---------------------------------------------")
print("special character exist: ")

# db table column name 가져와서 clear_col_name 이라는 list에 넣는 코드
query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}'"
cursor.execute(query)
column_names = cursor.fetchall()
clear_col_name = []
for col in column_names:
    clear_col_name.append(col[0])

print(clear_col_name)

query = "select * from choi_test2 where abc like '%\\\\n'"
cursor.execute(query)
column_names = cursor.fetchall()
# col_lst.append(column_names)
print(column_names)



exit()


# small change s s s s
# Define the table name
db_name = 'csv_db4'
table_name = 'choi_test2'
for column_name in clear_col_name:
    for column_patern in column_patterns:
        # Execute a query to get all column names from the table
        # OR 을 찾는 경우 theory, SELECT를 찾는 경우 SELECTION 같은 단어도 검출 되기 때문에, 데이터 잔존률을 유지하기 위해 SELECT, OR 같이 예약어와 정확히 일치하는 경우만 검출
        # 정확히 일치하는 단어는 하나밖에 없기 때문에 여러 개가 존재할 수 없음
        # print(column_patern)
        if column_patern == '\\\\n':     # \n인 경우만 따로 검사
            query = f"SELECT * FROM {table_name} WHERE {column_name} LIKE '%{column_patern}'"
            cursor.execute(query)
            column_names = cursor.fetchall()
            # col_lst.append(column_names)
            print(column_names)
            continue
        query = f"SELECT * FROM {table_name} WHERE {column_name} LIKE '%{column_patern}%'"
        cursor.execute(query)
        column_names = cursor.fetchall()
        # col_lst.append(column_names)
        print(column_names)

exit()

def col_special_char_check(column_patterns):
    col_lst = []
    for column_patern in column_patterns:
        # Execute a query to get all column names from the table
        # OR 을 찾는 경우 theory, SELECT를 찾는 경우 SELECTION 같은 단어도 검출 되기 때문에, 데이터 잔존률을 유지하기 위해 SELECT, OR 같이 예약어와 정확히 일치하는 경우만 검출
        # 정확히 일치하는 단어는 하나밖에 없기 때문에 여러 개가 존재할 수 없음
        # print(column_patern)
        if column_patern == '\\\\n':     # \n인 경우만 따로 검사
            query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}' AND LOWER(column_name) = '{column_patern}'"
            cursor.execute(query)
            column_names = cursor.fetchall()
            col_lst.append(column_names)
            # print(column_names)
            continue
        query = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{table_name}' AND LOWER(column_name) LIKE '%{column_patern}%'"
        cursor.execute(query)
        column_names = cursor.fetchall()
        col_lst.append(column_names)
        # print(column_names)
    return col_lst

print(col_special_char_check(column_patterns))

exit()

print()
print("---------------------------------------------")
print("special character exist: ")