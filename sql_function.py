import re
import pandas as pd
from tqdm import tqdm
import pandas as pd

def get_db_table(engine, database):
    try:
        query = 'DROP TABLE IF EXISTS csv_done_table;'
        engine.execute(query)

        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{database}' AND table_type = 'BASE TABLE'";
        table_names= engine.execute(query).fetchall()

        query = "create table if not exists csv_done_table(테이블_명 VARCHAR(255), 레코드_수 int);";
        engine.execute(query)

        for table_name in table_names:
            # query = f'insert into csv_done_table(테이블_명, 레코드_수) select table_name, TABLE_ROWS from INFORMATION_SCHEMA.tables where table_name="{table_name[0]}"';
            query = f'insert into csv_done_table(테이블_명) select table_name from INFORMATION_SCHEMA.tables where table_name= "{table_name[0]}" and table_schema = "{database}";'
            engine.execute(query)
            # query = f'insert into csv_done_table(레코드_수) select COUNT( * ) FROM `{table_name[0]}`'
            # query = f'update csv_done_table set 레코드_수 = (select COUNT( * ) FROM `{table_name[0]}` WHERE 테이블_명 =`{table_name[0]}` );'
            query = f'update csv_done_table set 레코드_수 = (select COUNT( * ) FROM `{table_name[0]}`) WHERE 테이블_명 like("{table_name[0]}")'
            engine.execute(query)

        query = f"SELECT * FROM csv_done_table;"

        result = engine.execute(query).fetchall()
        df = pd.DataFrame(result)

        query = 'DROP TABLE IF EXISTS csv_done_table;'
        engine.execute(query)
    except Exception as e:
        print('error :', e)

    return df, table_names

def change_word_null_sql(df, patterns):
    # Create the regular expression pattern
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)
    # Use the applymap() method to apply the re.sub() function to each element in the DataFrame
    result = df.applymap(lambda x: re.sub(pattern, '', x) if isinstance(x, str) else x)
    return result

def change_word_special_char_sql(df, patterns, engine, database, table_name):
    # Create the regular expression pattern
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)

    query = f'SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = "{database}" AND TABLE_NAME = "{table_name}";'
    table_type = engine.execute(query).fetchall()

    # print(df2)
    # col_type = ['TINYINT', 'SMALLINT','MEDIUMINT','INT','BIGINT','BIT','FLOAT','DOUBLE','DECIMAL']
    col_type = ['tinyint', 'smallint','mediumint','int','bigint','bit','float','double','decimal']

    numeric_col_index = []
    for i, col_type_ in enumerate(table_type):
        if col_type_[0].lower() in col_type:
            numeric_col_index.append(i)
        else:
            continue
    # print(numeric_col_index)
    if len(numeric_col_index) == 0:
        # print('there are none numeric type columns')
        return df
    else:  df.iloc[:, numeric_col_index] = df.iloc[:, numeric_col_index].applymap(lambda x: re.sub(pattern, '', x) if isinstance(x, str) else x)

    return df


def chunck_progress_bar_sql(table_name, engine, special_char, database):
    # print(special_char)
    query= f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{database}' AND table_name = 'modify_{table_name}'"
    col_names = engine.execute(query).fetchall()
    clear_col_name = []
    for col in col_names:
        clear_col_name.append(col[0])

    # print(clear_col_name)
    for spec_char in special_char:
        try:
            for i, col_name in enumerate(clear_col_name):
                if i == 0:
                    string = f"UPDATE modify_{table_name} SET {clear_col_name[i]} = REPLACE({clear_col_name[i]}, '{spec_char}', '')"
                else:
                    string += f", {clear_col_name[i]}= REPLACE({clear_col_name[i]}, '{spec_char}', '')"
            string += ';'
            # print(string)
            engine.execute(string)
        except Exception as e:
            print(e)

def chunck_progress_bar_sql_dup(table_name, engine, special_char, database):

    # print(special_char)
    query= f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{database}' AND table_name = '{table_name}'"
    col_names = engine.execute(query).fetchall()
    clear_col_name = []
    for col in col_names:
        clear_col_name.append(col[0])

    # print(clear_col_name)
    for spec_char in special_char:
        try:
            for i, col_name in enumerate(clear_col_name):
                if i == 0:
                    string = f"UPDATE {table_name} SET {clear_col_name[i]} = REPLACE({clear_col_name[i]}, '{spec_char}', '')"
                else:
                    string += f", {clear_col_name[i]}= REPLACE({clear_col_name[i]}, '{spec_char}', '')"
            string += ';'
            # print(string)
            engine.execute(string)
        except Exception as e:
            print(e)


def unique_value_col(table_name, database, engine):
# 속성명 가져오기
    query= f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{database}' AND table_name = '{table_name}'"
    col_names = engine.execute(query).fetchall()
    clear_col_name = []
    for col in col_names:
        clear_col_name.append(col[0])
    # print(clear_col_name)

    # # id 값 부여
    # query= 'SET @row_num = 0;'
    # engine.execute(query)
    # query= f'ALTER TABLE `{table_name}` ADD COLUMN IF NOT EXISTS rec_ord_num_ INT;'
    # engine.execute(query)
    # query= f'UPDATE `{table_name}` SET rec_ord_num_ = (@row_num := @row_num + 1);'
    # engine.execute(query)

    unique_value_count = []
    for col_name in clear_col_name:
        query = f'SELECT COUNT(DISTINCT {col_name}) FROM `{table_name}`;'
        unique_value = engine.execute(query).fetchone()[0]
        unique_value_count.append(unique_value)

    # print(unique_value_count)
        # unique_value_count

    # query = f'ALTER TABLE `{table_name}` DROP COLUMN `rec_ord_num_`;'
    # engine.execute(query)

    data = []
    for i, num_of_col in enumerate(unique_value_count):
        if num_of_col == 0 or num_of_col == 1:
            new_data = {'idx':i, 'col': clear_col_name[i]}
            data.append(new_data)
        else: continue

    return data

def distinct_value_prop(table_name, database, engine):
    query= f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{database}' AND table_name = '{table_name}'"
    col_names = engine.execute(query).fetchall()
    clear_col_name = []
    for col in col_names:
        clear_col_name.append(col[0])
    # print(clear_col_name)

    unique_value_count = []
    for col_name in clear_col_name:
        query = f'SELECT COUNT(DISTINCT {col_name}) FROM `{table_name}`;'
        unique_value = engine.execute(query).fetchone()[0]
        unique_value_count.append(unique_value)

    query = f'SELECT COUNT(*) FROM `{table_name}` ;'
    record_num =  engine.execute(query).fetchone()[0]

    distinct_value_prop = []

    for i, item in enumerate(unique_value_count):
        if item / record_num > 0.8:
            new_data = {'col': clear_col_name[i] ,'prop': '이상'}
            distinct_value_prop.append(new_data)
        else:
            new_data = {'col': clear_col_name[i] , 'prop': '이하'}
            distinct_value_prop.append(new_data)

    # print(distinct_value_prop)

    df = pd.DataFrame(distinct_value_prop)
    return df

def null_value_col(table_name, database, prop, engine):
# 속성명 가져오기
    query= f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{database}' AND table_name = '{table_name}'"
    col_names = engine.execute(query).fetchall()
    clear_col_name = []
    for col in col_names:
        clear_col_name.append(col[0])

    query = f"SELECT COUNT(*) FROM `{table_name}`"
    total_records = engine.execute(query).fetchone()[0]

    not_null_value_prop = []
    for col_name in clear_col_name:
        query = f'SELECT COUNT(*) FROM `{table_name}` WHERE {col_name} IS NULL;'
        not_null_value = engine.execute(query).fetchone()[0]
        not_null_value_prop.append(not_null_value/total_records)

    # print(unique_value_count)
        # unique_value_count

    # query = f'ALTER TABLE `{table_name}` DROP COLUMN `rec_ord_num_`;'
    # engine.execute(query)

    data = []
    for i, num_of_col in enumerate(not_null_value_prop):
        new_data = {'col': clear_col_name[i], 'null_prop': num_of_col}
        data.append(new_data)

    # df = pd.DataFrame(data)
    print(data)
    # for index, row in df.iterrows():
    #     if row['null_prop'] > prop:
    #         drop_query = f'ALTER TABLE `{table_name}` DROP COLUMN `{row["col"]}`;'
    #         engine.execute(drop_query)

    # return df


def get_iqr_table(table_name, database, engine, value_changed):

    # query = 'DROP TABLE IF EXISTS iqr_table;'
    # engine.execute(query)

    query= f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{database}' AND table_name = '{table_name}'"
    col_names = engine.execute(query).fetchall()
    clear_col_name = []
    for col in col_names:
        clear_col_name.append(col[0])

    query = f'SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = "{database}" AND TABLE_NAME = "{table_name}";'
    table_type = engine.execute(query).fetchall()

    # print(df2)
    # col_type = ['TINYINT', 'SMALLINT','MEDIUMINT','INT','BIGINT','BIT','FLOAT','DOUBLE','DECIMAL']
    col_type = ['tinyint', 'smallint','mediumint','int','bigint','bit','float','double','decimal']
    numeric_col_index = []
    numeric_col_name = []
    for i, col_type_ in enumerate(table_type):
        if col_type_[0].lower() in col_type:
            numeric_col_index.append(i)
            numeric_col_name.append(clear_col_name[i])
        else:
            continue

    if value_changed == True:
        # print('복제 테이블 생성 중...')
        # query = 'set @row_num := 0;'
        # engine.execute(query)
        # query = f'''create table modify_table
        # select @row_num := @row_num +1 as index_num_ber_ , a.*
        # from `{table_name}` as a ;'''
        # engine.execute(query)

        query = f"ALTER TABLE modify_{table_name} DROP COLUMN IF EXISTS index_num_ber_;"
        engine.execute(query)

        query = f'ALTER TABLE modify_{table_name} ADD COLUMN index_num_ber_ INT;'
        engine.execute(query)

        query = 'set @row_num := 0;'
        engine.execute(query)

        query = f'UPDATE modify_{table_name} SET index_num_ber_ = (@row_num := @row_num + 1);'
        engine.execute(query)

        query = 'DROP TABLE IF EXISTS modify_table;'
        engine.execute(query)

        print('복제 테이블 생성중...')
        query = f'''create table modify_table
        select a.*
        from `modify_{table_name}` as a ;'''
        engine.execute(query)

        query = f'ALTER TABLE modify_table ADD PRIMARY KEY (index_num_ber_);'
        engine.execute(query)

        query = 'ALTER TABLE modify_table ADD COLUMN new_co_lu_mn INT DEFAULT 1;'
        engine.execute(query)

        Q1_list = []
        Q3_list = []
        for col_name in numeric_col_name:
            query = f'SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col_name}) OVER (PARTITION BY new_co_lu_mn) FROM modify_table LIMIT 1;'
            Q1_value = engine.execute(query).fetchone()[0]
            Q1_list.append(Q1_value)

        for col_name in numeric_col_name:
            query = f'SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col_name}) OVER (PARTITION BY new_co_lu_mn) FROM modify_table LIMIT 1;'
            Q3_value = engine.execute(query).fetchone()[0]
            Q3_list.append(Q3_value)

        num_outlier_lst = []
        for idx, (q1,q3) in enumerate(zip(Q1_list, Q3_list)):
            IQR = q3 - q1
            lowest = q1 - 1.5 * IQR
            highest = q3 + 1.5 * IQR
            query = f'SELECT count(index_num_ber_) FROM modify_table WHERE {numeric_col_name[idx]} < {lowest} OR {numeric_col_name[idx]} > {highest};'
            num_outlier = engine.execute(query).fetchall()[0][0]
            num_outlier_lst.append(num_outlier)

            # lowest_list.append(lowest)
            # highest_list.append(highest)

        print('-----------------------------------')
        print('IQR Outlier 검사 결과')
        df = pd.DataFrame({
            '속성 번호': numeric_col_index,
            '속성명':numeric_col_name,
            'outlier 레코드 수':num_outlier_lst,
            '제거여부': False
        })
        # print(df)

        deleted_set = set()

        while True:
            print(df)
            print('outlier 레코드를 제거하고 싶은 *인덱스 번호*를 입력하시오. (종료 원할 시 -1를 입력해주십시오)')
            user_input = int(input('인덱스 번호 : '))
            try:
                if user_input in range(len(numeric_col_index)):
                    print('레코드 삭제 중...')
                    q1 = Q1_list[user_input]
                    q3 = Q3_list[user_input]
                    IQR = q3 - q1
                    lowest = q1 - 1.5 * IQR
                    highest = q3 + 1.5 * IQR
                    query = f'SELECT index_num_ber_ FROM modify_table WHERE {numeric_col_name[user_input]} < {lowest} OR {numeric_col_name[user_input]} > {highest};'
                    num_outlier = engine.execute(query).fetchall()
                    num_outlier_set = set([x[0] for x in num_outlier])
                    num_outlier_set = num_outlier_set - deleted_set
                    deleted_set.update(num_outlier_set)
                    # print(tuple(num_outlier_set)[0])
                    # print(len(tuple(num_outlier_set)))
                    if len(tuple(num_outlier_set)) == 1:
                        query = f'DELETE FROM modify_table WHERE index_num_ber_ IN ({tuple(num_outlier_set)[0]});'
                        num_outlier = engine.execute(query)
                        print('제거되었습니다.')
                        df['제거여부'][user_input] = True
                    elif len(tuple(num_outlier_set)) == 0:
                        print('제거할 레코드가 없습니다.')
                        df['제거여부'][user_input] = True
                    else:
                        query = f'DELETE FROM modify_table WHERE index_num_ber_ IN {tuple(num_outlier_set)};'
                        num_outlier = engine.execute(query)
                        print('제거되었습니다.')
                        df['제거여부'][user_input] = True

                    # print(num_outlier_set)
                    # print(deleted_set)
                    # print(num_outlier)
                elif user_input == -1:
                    break
                else:
                    print("다시 입력해주십시오.")
            except Exception as e:
                print('error :', e)

        query = f'DROP TABLE IF EXISTS modify_{table_name};'
        engine.execute(query)

        query = f'RENAME TABLE modify_table TO modify_{table_name};'
        engine.execute(query)

        query = f'ALTER TABLE `modify_{table_name}` DROP COLUMN `new_co_lu_mn`;'
        engine.execute(query)

        print(f'db에 modify_{table_name} 테이블이 생성되었습니다.')

    else:
        query = f'ALTER TABLE {table_name} ADD COLUMN new_co_lu_mn INT DEFAULT 1;'
        engine.execute(query)

        Q1_list = []
        Q3_list = []
        for col_name in numeric_col_name:
            query = f'SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col_name}) OVER (PARTITION BY new_co_lu_mn) FROM {table_name} LIMIT 1;'
            Q1_value = engine.execute(query).fetchone()[0]
            Q1_list.append(Q1_value)

        for col_name in numeric_col_name:
            query = f'SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col_name}) OVER (PARTITION BY new_co_lu_mn) FROM {table_name} LIMIT 1;'
            Q3_value = engine.execute(query).fetchone()[0]
            Q3_list.append(Q3_value)

        num_outlier_lst = []
        for idx, (q1,q3) in enumerate(zip(Q1_list, Q3_list)):
            IQR = q3 - q1
            lowest = q1 - 1.5 * IQR
            highest = q3 + 1.5 * IQR
            query = f'SELECT count(index_num_ber_) FROM {table_name} WHERE {numeric_col_name[idx]} < {lowest} OR {numeric_col_name[idx]} > {highest};'
            num_outlier = engine.execute(query).fetchall()[0][0]
            num_outlier_lst.append(num_outlier)

            # lowest_list.append(lowest)
            # highest_list.append(highest)

        print('-----------------------------------')
        print('IQR Outlier 검사 결과')
        df = pd.DataFrame({
            '속성 번호': numeric_col_index,
            '속성명':numeric_col_name,
            'outlier 레코드 수':num_outlier_lst,
            '제거여부': False
        })
        # print(df)

        deleted_set = set()

        while True:
            print(df)
            print('outlier 레코드를 제거하고 싶은 *인덱스 번호*를 입력하시오. (종료 원할 시 -1를 입력해주십시오)')
            user_input = int(input('인덱스 번호 : '))
            try:
                if user_input in range(len(numeric_col_index)):
                    print('레코드 삭제 중...')
                    q1 = Q1_list[user_input]
                    q3 = Q3_list[user_input]
                    IQR = q3 - q1
                    lowest = q1 - 1.5 * IQR
                    highest = q3 + 1.5 * IQR
                    query = f'SELECT index_num_ber_ FROM {table_name} WHERE {numeric_col_name[user_input]} < {lowest} OR {numeric_col_name[user_input]} > {highest};'
                    num_outlier = engine.execute(query).fetchall()
                    num_outlier_set = set([x[0] for x in num_outlier])
                    num_outlier_set = num_outlier_set - deleted_set
                    deleted_set.update(num_outlier_set)
                    # print(tuple(num_outlier_set)[0])
                    # print(len(tuple(num_outlier_set)))
                    if len(tuple(num_outlier_set)) == 1:
                        query = f'DELETE FROM {table_name} WHERE index_num_ber_ IN ({tuple(num_outlier_set)[0]});'
                        num_outlier = engine.execute(query)
                        print('제거되었습니다.')
                        df['제거여부'][user_input] = True
                    elif len(tuple(num_outlier_set)) == 0:
                        print('제거할 레코드가 없습니다.')
                        df['제거여부'][user_input] = True
                    else:
                        query = f'DELETE FROM {table_name} WHERE index_num_ber_ IN {tuple(num_outlier_set)};'
                        num_outlier = engine.execute(query)
                        print('제거되었습니다.')
                        df['제거여부'][user_input] = True

                    # print(num_outlier_set)
                    # print(deleted_set)
                    # print(num_outlier)
                elif user_input == -1:
                    break
                else:
                    print("다시 입력해주십시오.")
            except Exception as e:
                print('error :', e)

        # query = f'RENAME TABLE modify_{table_name} TO iqr_modify_{table_name};'
        # engine.execute(query)
        query = f'ALTER TABLE `{table_name}` DROP COLUMN `new_co_lu_mn`;'
        engine.execute(query)
        # print(f'db에 iqr_modify_{table_name} 테이블이 생성되었습니다.')



    # print(clear_col_name)
    # print(table_type)
    # print(numeric_col_name)
    # print(numeric_col_index)




    # query = "create table if not exists csv_done_table(테이블_명 VARCHAR(255), 레코드_수 int);";
    # engine.execute(query)

    # for table_name in table_names:
    #     # query = f'insert into csv_done_table(테이블_명, 레코드_수) select table_name, TABLE_ROWS from INFORMATION_SCHEMA.tables where table_name="{table_name[0]}"';
    #     query = f'insert into csv_done_table(테이블_명) select table_name from INFORMATION_SCHEMA.tables where table_name= "{table_name[0]}" and table_schema = "{database}";'
    #     engine.execute(query)
    #     # query = f'insert into csv_done_table(레코드_수) select COUNT( * ) FROM `{table_name[0]}`'
    #     # query = f'update csv_done_table set 레코드_수 = (select COUNT( * ) FROM `{table_name[0]}` WHERE 테이블_명 =`{table_name[0]}` );'
    #     query = f'update csv_done_table set 레코드_수 = (select COUNT( * ) FROM `{table_name[0]}` ) WHERE 테이블_명 like("{table_name[0]}")'
    #     engine.execute(query)

    # query = f"SELECT * FROM csv_done_table;"

    # result = engine.execute(query).fetchall()
    # df = pd.DataFrame(result)
    # return
