import pandas as pd
import re
import mariadb
import pymysql
from sqlalchemy import create_engine
import glob
from multiprocessing import Pool
from multiprocessing import freeze_support
import dask.dataframe as dd

# The DataFrame of strings
df = pd.DataFrame([[1, "'2", 'select',1,2,3,4,5,6,7], [4, None, '\n as',1,2,3,4,5,6,7], [7, ' ', 9,1,2,3,4,5,6,7], [None, None, None,1,2,3,4,5,6,7]], columns=[' ex','{','2test','3test','select','test','test','name','name','name'])

# The patterns we want to find 특수문자나 예약어 등록해놓으면
column_patterns = ['"', "'", '{', '}', ' ']
value_patterns = ['"', "'", '{', '}']
reserved_words = ['select', 'from', 'where']
null_words = ['\n']

def check_word(df, patterns):       # patterns가 속성값에 있으면 True, 없으면 False 반환하는 함수
    # Use the applymap() method to apply a lambda function to each element in the DataFrame
    result = df.applymap(lambda x: isinstance(x, str) and any(pattern.lower() in x.lower() for pattern in patterns))
    return result


def change_word(df, patterns):      # patterns가 속성값에 있으면 pattern 부분만 '_'로 바꿔주는 함수
    #for index_test in list(df.columns): print(df[index_test].dtypes) #check columns dtype
    # Create the regular expression pattern
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)
    # Use the applymap() method to apply the re.sub() function to each element in the DataFrame
    result = df.applymap(lambda x: re.sub(pattern, '_', x) if isinstance(x, str) else x)
    return result


def change_word_null(df, patterns):
    # Create the regular expression pattern
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)
    # Use the applymap() method to apply the re.sub() function to each element in the DataFrame
    result = df.applymap(lambda x: re.sub(pattern, '', x) if isinstance(x, str) else x)
    return result


def change_column(df, patterns):
    #컬럼명에 특수문자, 공백이 있는지 검사 후 값 변경
    df_col = list(df.columns)
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)
    for i in range(len(df_col)):
        if isinstance(df_col[i], str) and re.search(pattern,df_col[i]):
            df_col[i] = 'col_{}'.format(i+1)
    df.columns = df_col

    # 컬럼명이 숫자로 시작하는 지 검사 후 값 변경
    df_col2 = list(df.columns)
    num_pattern = r"^\d"
    regex = re.compile(num_pattern)
    # Iterate over the list of columns
    for columns in df_col2:
    # Use the search function to check if the columns starts with a number
        match = regex.search(columns)
        if match:
        # Use the sub function to replace the columns with 'col'
            modified_column = regex.sub('col', columns)
            df.rename(columns = {columns:modified_column},inplace=True)
        else: continue


    # 컬럼명이 중복될 경우 중복된 열 이름 변경
    # Check for duplicate column names
    duplicated = df.columns.duplicated()
    if True in duplicated:
        # Create a list of duplicate column names
        duplicate_columns = [[i, column] for i, column in enumerate(df.columns) if duplicated[i]]
        # Rename the duplicate columns
        counter = 2
        df_col = list(df.columns)
        # print(duplicate_columns)
        #for tuple in duplicated_colomn 이렇게 수정하기
        last_value = ''
        for idx, col in duplicate_columns:
            # print(idx, col)
            if col != last_value: counter = 2
            df_col[idx] =  df_col[idx] + str(counter)
            counter += 1
            last_value = col        #이전 컬럼명과 같은지 체크하는 기능
        df.columns = df_col

    return df
# patterns가 컬럼명에 있으면 pattern 부분을 다음과 같이 바꿔주는 함수
# 1. 컬럼명에 예약어, 특수문자, 공백이 있는 경우 col_{컬럼번호} 이렇게 바꿔준다
# 2. 컬럼명이 숫자로 시작될 경우 숫자를 col로 바꿔준다 (공백으로 처리하지 않은 이유는 숫자만 있는 경우 공백으로 바꾸면 안되기 때문이다)
# 3. 컬럼명이 중복될 경우 중복된 열 이름을 바꿔준다. ex) test test test name name -> test test2 test3 name name2 이렇게 바꿔준다

def add_record(df):
    df.insert(0, 'record_number', range(1, len(df) + 1), True)
    return df

df = change_column(df, column_patterns)
# print("---------------------------------")
# print("column_change")
# print(df)

df = change_column(df, reserved_words)
# print("---------------------------------")
# print("column_change : reserved words")
# print(df)


df = change_word(df, value_patterns)
# print("---------------------------------")
# print("word_change")
# print(df)

df = change_word(df, reserved_words)
print("---------------------------------")
print("word_change : reserved words")
print(df)

df = change_word_null(df, null_words)
print("---------------------------------")
print("word_change : null words")
print(df)

df = add_record(df)
print("---------------------------------")
print("add record column")
print(df)



#db에 밀어넣기
# if True:
#     host = "127.0.0.1"
#     user = "root"
#     password = "0000"
#     port = "3306"
#     database = "test_db"                  #넣을 db이름

#     engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', encoding='utf-8')

#     df.to_sql(index = False,
#             name = 'df',
#             con = engine,
#             if_exists = 'append',
#             method = 'multi',
#             chunksize = 10000)
