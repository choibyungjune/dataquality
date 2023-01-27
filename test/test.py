# import pandas as pd

# df = pd.DataFrame([])


# df = pd.read_csv("C:/Users/Yonsei/Downloads/소상공인시장진흥공단_상가(상권)정보_20220930/soho_seoul.csv")

# ## Fastest would be using length of index
# print("전체 행 : ", len(df.index))
 
# ## If you want the column and row count then
# row_count, column_count = df.shape
# print("총 행(row) 수 : ", row_count)
# print("총 칼럼(열) 수 : ", column_count)
 
# # 데이터 셋 df의 종합적인 정보는 df.info( ) 함수를 통해 확인 가능
# # Dtype의 int64는 정수, object는 문자열, float64는 실수를 의미
# print(df.info())
 
# # 데이터 프레임에서 결측치를 True, 값이 있으면 False를 반환
# print(df.isnull())
 
# # 각 column들이 몇개의 null값을 가졌는지 확인
# is_null = df.isnull().sum()
# print(is_null)
# #데이터 유형 : varchar int double float 중에서 사용할 수 있도록 지정하시면 됩니다.

# #저장하실때 utf-8 쉼표로 구분 


# # 위에서부터 지정된 개수만큼 출력하기
# df_head = df.head(10) # 개수를 지정하지 않으면 기본 5개 출력
# print(df_head)
 
# # 3 ~ 6번째 데이터 출력, 칼럼(열)은 10번째 열까지 출력
# print(df.iloc[2:7,:10])
 
# # 열 여러개 선택하기
# new_df = df.head(20)[['상호명','상권업종대분류명','상권업종중분류명','상권업종소분류명','표준산업분류명','시군구명','행정동명','건물명']]
# print(new_df)
 
# # 맨 하단 10개 데이터를 칼럼(열) 10개만 출력
# print(df.tail(10).iloc[:,:10])
 
# print(df.tail(10)[['상가업소번호','상호명','상권업종대분류명','상권업종중분류명','상권업종소분류명','표준산업분류명','시군구명','행정동명','건물명']])
 
#  # 특정 칼럼 중복 제거
# item = df['상권업종대분류명'].drop_duplicates()
# print(item)
 
# # LIST로 변환
# item_list = item.values.tolist()
# print(item_list)
 
# # 가나다순 정렬
# varlist = sorted(item_list)
# print(varlist)
 
# LIST to DB Insert

import pandas as pd
import mariadb
import sys
 
# Connect to MariaDB Platform
try:
    mydb = mariadb.connect(
        user="root",
        password="0000",
        host="127.0.0.1",
        port=3306,
        database="csv_db3"
 
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)
 
# Get Cursor
dbconn = mydb.cursor()
 
 
# data = pd.read_csv('soho_seoul.csv', header=None) # KeyError: '상권업종대분류명'
# df = pd.read_csv('soho_seoul.csv') # KeyError: '상권업종대분류명'

df = pd.read_csv("C:/Users/Yonsei/Downloads/소상공인시장진흥공단_상가(상권)정보_20220930/soho_seoul.csv")
 
## Fastest would be using length of index
print("전체 행 : ", len(df.index))
 
## If you want the column and row count then
row_count, column_count = df.shape
print("총 행(row) 수 : ", row_count)
print("총 칼럼(열) 수 : ", column_count)
 
# 특정 칼럼 중복 제거
item = df['상권업종대분류명'].drop_duplicates()
print(item)
 
# LIST로 변환
item_list = item.values.tolist()
print(item_list)
 
# 가나다순 정렬
varlist = sorted(item_list)
print(varlist)
 
# 배열 사이즈 구하기
print(len(varlist))
 
# 배열 사이즈만큼 동일한 값 초기화, 두가지 방법 모두 가능
parent_id = [0 for i in range(len(varlist))]
depth = [1] * len(varlist)
 
# Series 생성 및 DataFrame 전환 <== mariaDB 입력 데이터 생성 목적
df_parentid = pd.Series(parent_id)
df_depth = pd.Series(depth)
df_name = pd.Series(varlist)
 
df_all = pd.concat([df_parentid, df_depth, df_name, df_name], axis=1)
print(df_all)
 
# DataFrame to List
db_itemlist = df_all.values.tolist()
print(db_itemlist)
 
# LIST to mariaDB Insert
sql = "INSERT INTO cate(parent_id,depth,name,text) VALUES(%s,%s,%s,%s)"
dbconn.executemany(sql,db_itemlist)
 
