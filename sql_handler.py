import pandas as pd
from sqlalchemy import create_engine
from sql_function import get_db_table, change_word_null_sql, change_word_special_char_sql, chunck_progress_bar_sql, unique_value_col, distinct_value_prop, null_value_col, get_iqr_table, chunck_progress_bar_sql_dup
from csv_function import special_char_list, reserve_word_list, change_col_special_char, change_col_reserved_words, change_col_start_num

def sql_handler():

    # column_patterns = ['\\\\n','\n','\\n','\\\\','"', "'", '~', '{', '}','「','(',')', ' ','.','!','@','#','$','%','^','&','\\(','\\)','\\*']
    reserved_words = ['select', 'from', 'where','rec_disim','rec_numerical','rec_categorical']
    special_char = ['!','@','#','\\$','%','^','&','\\(','\\)','\\*','\\?','\\-','\\~']
    sql_special_char = ['!','@','#','$','&','%%','^','(',')','*','?','-','~']           # %를 %%로 넣어야 query 날릴때 에러 안남

    print('데이터베이스를 연결합니다.')
    while True:
        # host = input('H O S T : ')
        # port = input('P O R T : ')
        # database = input('DATABASE : ')
        # user = input('U S E R : ')
        # password = input('PASSWORD : ')

        host = '127.0.0.1'
        port = '3306'
        database = 'hyo_king'
        user = 'root'
        password = '0000'

        try:
            engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', encoding='utf-8')
            conn = engine.connect()
            print("연결 성공했습니다")
            conn.close()
            break
        except Exception as e:
            print("다시 입력해주십시오.")


    print('-----------------------------------')
    print('테이블 목록')
    df, table_names = get_db_table(engine, database)

    print(df)
    # print(table_names)

    while True:
        idx_choice = int(input('검사할 테이블 인덱스(번호)를 입력하시오:'))
        try:
            user_input = input(f'선택된 테이블은 {table_names[idx_choice][0]}입니다. 맞습니까?(Y/N)')
            if user_input.lower() == 'y' or user_input.lower() == 'yes':
                break
            elif user_input.lower() == 'n' or user_input.lower() == 'no':
                print("다시 입력해주십시오")
            else:
                print("다시 입력해주십시오")
        except Exception as e:
            print('error :', e)

    while True:
        user_input = input('원본 데이터를 보존하시겠습니까?(보존:Y, 수정:N)')
        try:
            if user_input.lower() == 'y' or user_input.lower() == 'yes':
                duplicate = True
                break
            elif user_input.lower() == 'n' or user_input.lower() == 'no':
                duplicate = False
                break
            else:
                print("다시 입력해주십시오")
        except Exception as e:
            print('error :', e)

    table_name = table_names[idx_choice][0]

    one = '1. 속성명 검사'
    two = '2. 속성값 특수문자 검사'
    three = '3. 속성값 특이 검사'

    value_changed = False

    if duplicate == True:
        print('복제 테이블 생성 중...')
        query = f'''create table IF NOT EXISTS modify_{table_name}
        select a.*
        from `{table_name}` as a ;'''
        engine.execute(query)

        while True:
            input_num = input(f'''어떤 작업을 하시겠습니까?(번호만 입력)
                {one}
                {two}
                {three}
                4. 특수 문자 지정
                5. 예약어 지정
                6. 종료
                ※ 변경사항은 데이터베이스에 자동 반영됩니다.
                    ''')
            query = f"SELECT * FROM `modify_{table_name}` LIMIT 5;"
            col_type = engine.execute(query).fetchall()
            df = pd.DataFrame(col_type)

            if input_num == '1':
                try:
                    old_names = df.columns
                    print('-----------------------------------')
                    print('0. 기존 데이터')
                    print('-----------------------------------')
                    print(df.head())

                    print('-----------------------------------')
                    print('1. 특수 문자 존재 검사')
                    df = change_col_special_char(df, special_char)
                    # print(df.head())

                    print('-----------------------------------')
                    print('2. 예약어 존재 검사')
                    df = change_col_reserved_words(df, reserved_words)
                    # print(df.head())

                    print('-----------------------------------')
                    print('3. 숫자로 속성명 시작 여부 검사')
                    df = change_col_start_num(df)
                    # print(df.head())

                    new_names = df.columns

                    query = f'SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = "{database}" AND TABLE_NAME = "modify_{table_name}";'
                    table_type = engine.execute(query).fetchall()
                    # print(table_type)

                    for idx, (old_name, new_name) in enumerate(zip(old_names, new_names)):
                        # print(idx, (old_name, new_name))
                        # print(old_name)
                        # print(new_name)
                        if old_name != new_name:
                            if table_type[idx][0] == 'varchar':
                                query = f'''SELECT CHARACTER_MAXIMUM_LENGTH
                                            FROM INFORMATION_SCHEMA.COLUMNS
                                            WHERE TABLE_SCHEMA = '{database}'
                                            AND TABLE_NAME = '{table_name}'
                                            AND COLUMN_NAME = '{old_name}'
                                            AND DATA_TYPE = 'varchar';'''
                                varchar_length = engine.execute(query).fetchall()[0][0]
                                query = f"ALTER TABLE `modify_{table_name}` CHANGE COLUMN `{old_name}` `{new_name}` varchar({varchar_length})"
                                engine.execute(query)
                                # print(varchar_length)
                            else:
                                query = f"ALTER TABLE `modify_{table_name}` CHANGE COLUMN `{old_name}` `{new_name}` {table_type[idx][0]}"
                                engine.execute(query)
                        else:
                            continue
                        # print(idx)
                    one = one + '(완료)'
                    continue
                except Exception as e:
                    print('error :',e)
            elif input_num == '2':
                while True:
                    input_num_value = input(f'''어떤 작업을 하시겠습니까?(번호만 입력)
                    1. 범주형, 수치형 데이터 특수문자 제거
                    2. 수치형 데이터 특수문자 제거
                                            ''')
                    if input_num_value == '1':
                        chunck_progress_bar_sql(table_name, engine, sql_special_char, database)
                        two = two + '(수치형, 범주형 완료)'
                        break
                    elif input_num_value == '2':
                        chunck_progress_bar_sql(table_name, engine, sql_special_char, database)
                        two = two + '(수치형 완료)'
                        break
                    else:
                        print('다시 입력해주십시오')
                continue
            elif input_num == '3':
                print('-----------------------------------')
                print('0. 기존 데이터')
                print('-----------------------------------')
                print(df.head())

                print('-----------------------------------')
                print('1. 속성값 모두 동일 여부 검사')
                print('검사중...')
                same_lst = unique_value_col(table_name, database, engine)
                print(same_lst)

                print('-----------------------------------')
                print('2. 속성의 distinct 값 비율이 80% 이하인 속성 검사')
                print('검사중...')
                prop_df = distinct_value_prop(table_name, database, engine)
                print(prop_df)

                print('-----------------------------------')
                print('3. 속성의 null 값 비율 검사 및 제거')
                prop = float(input('임계치를 지정해주세요.(ex. 80%면 0.8입력): '))
                print('검사중...')
                null_value_col(table_name, database, prop, engine)
                # print(prop_df2)
                # print('-----------------------------------')
                # print('3. 속성의 NULL 을 제외한 값의 비율을 확인')
                # print(check_not_null_prop(df))
                print('-----------------------------------')
                print('4. 수치형 속성의 IQR 이상치 검사 및 제거')
                print('검사중...')
                # df_iqr = get_iqr_table(table_name, database, engine)
                get_iqr_table(table_name, database, engine, duplicate)
                iqr_changed = True
                # print(df_iqr)


                three = three + '(완료)'
                continue
            elif input_num == '4':
                special_char, sql_special_char = special_char_list(special_char, sql_special_char)

            elif input_num == '5':
                reserved_words = reserve_word_list(reserved_words)

            elif input_num == '6':
                try:
                    exit()
                except Exception as e:
                    print('종료합니다')

    else:
        while True:
            input_num = input(f'''어떤 작업을 하시겠습니까?(번호만 입력)
                {one}
                {two}
                {three}
                4. 특수 문자 지정
                5. 예약어 지정
                6. 종료
                ※ 변경사항은 데이터베이스에 자동 반영됩니다.
                    ''')
            query = f"SELECT * FROM `{table_name}` LIMIT 5;"
            col_type = engine.execute(query).fetchall()
            df = pd.DataFrame(col_type)

            if input_num == '1':
                try:
                    old_names = df.columns
                    print('-----------------------------------')
                    print('0. 기존 데이터')
                    print('-----------------------------------')
                    print(df.head())

                    print('-----------------------------------')
                    print('1. 특수 문자 존재 검사')
                    df = change_col_special_char(df, special_char)
                    # print(df.head())

                    print('-----------------------------------')
                    print('2. 예약어 존재 검사')
                    df = change_col_reserved_words(df, reserved_words)
                    # print(df.head())

                    print('-----------------------------------')
                    print('3. 숫자로 속성명 시작 여부 검사')
                    df = change_col_start_num(df)
                    # print(df.head())

                    new_names = df.columns

                    query = f'SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = "{database}" AND TABLE_NAME = "{table_name}";'
                    table_type = engine.execute(query).fetchall()
                    # print(table_type)

                    for idx, (old_name, new_name) in enumerate(zip(old_names, new_names)):
                        # print(idx, (old_name, new_name))
                        # print(old_name)
                        # print(new_name)
                        if old_name != new_name:
                            if table_type[idx][0] == 'varchar':
                                query = f'''SELECT CHARACTER_MAXIMUM_LENGTH
                                            FROM INFORMATION_SCHEMA.COLUMNS
                                            WHERE TABLE_SCHEMA = '{database}'
                                            AND TABLE_NAME = '{table_name}'
                                            AND COLUMN_NAME = '{old_name}'
                                            AND DATA_TYPE = 'varchar';'''
                                varchar_length = engine.execute(query).fetchall()[0][0]
                                query = f"ALTER TABLE `{table_name}` CHANGE COLUMN `{old_name}` `{new_name}` varchar({varchar_length})"
                                engine.execute(query)
                                # print(varchar_length)
                            else:
                                query = f"ALTER TABLE `{table_name}` CHANGE COLUMN `{old_name}` `{new_name}` {table_type[idx][0]}"
                                engine.execute(query)
                        else:
                            continue
                        # print(idx)
                    one = one + '(완료)'
                    continue
                except Exception as e:
                    print('error :',e)
            elif input_num == '2':
                while True:
                    input_num_value = input(f'''어떤 작업을 하시겠습니까?(번호만 입력)
                    1. 범주형, 수치형 데이터 특수문자 제거
                    2. 수치형 데이터 특수문자 제거
                                            ''')
                    if input_num_value == '1':
                        chunck_progress_bar_sql_dup(table_name, engine, sql_special_char, database)
                        two = two + '(수치형, 범주형 완료)'
                        value_changed = True
                        break
                    elif input_num_value == '2':
                        chunck_progress_bar_sql_dup(table_name, engine, sql_special_char, database)
                        two = two + '(수치형 완료)'
                        value_changed = True
                        break
                    else:
                        print('다시 입력해주십시오')
                continue
            elif input_num == '3':
                print('-----------------------------------')
                print('0. 기존 데이터')
                print('-----------------------------------')
                print(df.head())

                print('-----------------------------------')
                print('1. 속성값 모두 동일 여부 검사')
                print('검사중...')
                same_lst = unique_value_col(table_name, database, engine)
                print(same_lst)

                print('-----------------------------------')
                print('2. 속성의 distinct 값 비율이 80% 이하인 속성 검사')
                print('검사중...')
                prop_df = distinct_value_prop(table_name, database, engine)
                print(prop_df)

                print('-----------------------------------')
                print('3. 속성의 null 제외한 값 비율 검사')
                prop = float(input('임계치를 지정해주세요.(ex. 80%면 0.8입력): '))
                print('검사중...')
                null_value_col(table_name, database, prop, engine)
                # print(prop_df2)
                # print('-----------------------------------')
                # print('3. 속성의 NULL 을 제외한 값의 비율을 확인')
                # print(check_not_null_prop(df))
                print('-----------------------------------')
                print('4. 수치형 속성의 IQR 이상치 검사 및 제거')
                print('검사중...')
                # df_iqr = get_iqr_table(table_name, database, engine)
                get_iqr_table(table_name, database, engine, duplicate)
                # print(df_iqr)


                three = three + '(완료)'
                continue
            elif input_num == '4':
                special_char, sql_special_char = special_char_list(special_char, sql_special_char)

            elif input_num == '5':
                reserved_words = reserve_word_list(reserved_words)

            elif input_num == '6':
                try:
                    exit()
                except Exception as e:
                    print('종료합니다')







    # while True:
    #     try:
    #         file_name = input('파일 명(파일 주소)를 입력해주세요')
    #         df = pd.read_csv(f'{file_name}')
    #         break  # exit the loop if no exception is raised
    #     except Exception as e:
    #         print("파일 인풋 시, 에러가 발생했습니다:", e)


    # # df = pd.read_csv(f'{file_name}')
    # print(df.head())

    # column_patterns = ['"', "'", '~', '{', '}','「','(',')', ' ','.','!','@','#','$','%','^','&','\\(','\\)','\\*']
    # reserved_words = ['select', 'from', 'where','index_num_ber_','rec_disim','rec_numerical','rec_categorical']
    # special_char = ['\\\\n','\n','\\n','\\\\','!','@','#','$','%','^','&','\\(','\\)','\\*']

    # one = '1. 속성명 검사'
    # two = '2. 속성값 검사'
    # three = '3. 속성 검사'

    # while True:
    #     input_num = input(f'''어떤 작업을 하시겠습니까?(번호만 입력)
    #     {one}
    #     {two}
    #     {three}
    #     4. 변경된 csv 파일 다운로드
    #     5. 종료
    #         ''')
    #     if input_num == '1':

    #         print('-----------------------------------')
    #         print('0. 기존 데이터')
    #         print('-----------------------------------')
    #         print(df.head())

    #         print('-----------------------------------')
    #         print('1. 속성 타입 유효성 검사')
    #         df = check_col_type(df)
    #         print(df.head())

    #         print('-----------------------------------')
    #         print('2. 특수 문자 존재 검사')
    #         df = change_col_special_char(df, column_patterns)
    #         print(df.head())

    #         print('-----------------------------------')
    #         print('3. 예약어 존재 검사')
    #         df = change_col_reserved_words(df, reserved_words)
    #         print(df.head())

    #         print('-----------------------------------')
    #         print('4. 숫자로 속성명 시작 여부 검사')
    #         df = change_col_start_num(df)
    #         print(df.head())

    #         one = one + '(완료)'
    #         continue

    #     elif input_num == '2':
    #         input_num_value = input(f'''어떤 작업을 하시겠습니까?(번호만 입력)
    #         1. 범주형, 수치형 데이터 특수문자 제거
    #         2. 수치형 데이터 특수문자 제거
    #                                 ''')
    #         if input_num_value == '1':
    #             df = chunck_progress_bar(df, df.shape[0]//10, change_word_null, special_char)
    #             two = two + '(수치형, 범주형 완료)'
    #         elif input_num_value == '2':
    #             df = chunck_progress_bar(df, df.shape[0]//10, change_word_special_char, special_char)
    #             two = two + '(수치형 완료)'
    #         else:
    #             print('다시 입력해주십시오')
    #         continue

    #     elif input_num == '3':
    #         print('-----------------------------------')
    #         print('0. 기존 데이터')
    #         print('-----------------------------------')
    #         print(df.head())

    #         print('-----------------------------------')
    #         print('1. 속성값 모두 동일 여부 검사')
    #         df = num_of_unique_col(df)
    #         print(df.head())

    #         print('-----------------------------------')
    #         print('2. 속성의 distinct 값 비율이 80% 이하인 속성 검사')
    #         prop_lst = check_distinct_prop(df, 0.8)
    #         print(prop_lst)

    #         print('-----------------------------------')
    #         print('3. 속성의 NULL 을 제외한 값의 비율을 확인')
    #         print(check_not_null_prop(df))

    #         three = three + '(완료)'
    #         continue

    #     elif input_num == '4':
    #         df.to_csv(f'modify_{file_name}', index=False)
    #         print('-----------------------------------')
    #         print('다운로드 완료됐습니다')
    #         continue

    #     elif input_num == '5':
    #         print('종료합니다')
    #         exit()
