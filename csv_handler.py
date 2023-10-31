import pandas as pd
import os
from csv_function import change_col_type, special_char_list, reserve_word_list, change_col_special_char, change_col_reserved_words, change_col_start_num, chunck_progress_bar, change_word_null, change_word_special_char, num_of_unique_col, check_distinct_prop, check_null_prop_delete, check_not_null_prop

def csv_handler():
    while True:
        try:
            file_name = input('파일 명(파일 주소)를 입력해주세요')
            if os.path.exists(file_name):
                print("파일이 존재합니다.")
                break
            else:
                print("파일이 존재하지 않습니다.")
        except Exception as e:
            print("파일 인풋 시, 에러가 발생했습니다:", e)

    # print(file_name)

    chunksize = int(input('chunksize를 지정해주세요: '))

    with open(file_name, 'r', encoding='utf-8') as file:
        col_name = file.readline().strip().split(',')
        col_type = file.readline().strip().split(',')
    # print(col_name)
    # print(col_type)

    df = pd.DataFrame([col_type], columns=col_name)
    # print(df.head())
    # column_patterns = ['\\\\n','\n','\\n','\\\\','"', "'", '~', '{', '}','「','(',')', ' ','.','!','@','#','$','%','^','&','\\(','\\)','\\*']
    reserved_words = ['select', 'from', 'where','index_num_ber_','rec_disim','rec_numerical','rec_categorical']
    special_char = ['!','@','#','$','%','^','&','\\(','\\)','\\*']

    one = '1. 속성명 검사'
    two = '2. 속성값 검사'
    three = '3. 속성 검사'

    while True:
        input_num = input(f'''어떤 작업을 하시겠습니까?(번호만 입력)
        {one}
        {two}
        {three}
        4. 특수 문자 지정
        5. 예약어 지정
        6. 종료
            ''')
        if input_num == '1':

            # print('-----------------------------------')
            # print('0. 기존 데이터')
            # print('-----------------------------------')
            # print(df.head())

            print('-----------------------------------')
            print('1. 속성 타입 유효성 검사')
            df = change_col_type(df)

            print('-----------------------------------')
            print('2. 특수 문자 존재 검사')
            df = change_col_special_char(df, special_char)

            print('-----------------------------------')
            print('3. 예약어 존재 검사')
            df = change_col_reserved_words(df, reserved_words)

            print('-----------------------------------')
            print('4. 숫자로 속성명 시작 여부 검사')
            df = change_col_start_num(df)


            print('속성명에 이상잉 없습니다.')
            one = one + '(완료)'
            continue

        elif input_num == '2':
            print('-----------------------------------')
            print('특수문자 변경 후 파일 저장')
            chunck_progress_bar(file_name, change_word_null, special_char, chunksize)

            print('작업이 끝났습니다. 종료합니다.')
            os.system("pause")
            exit()



        elif input_num == '3':
            # print('-----------------------------------')
            # print('0. 기존 데이터')
            # print('-----------------------------------')
            # print(df.head())
            # df = pd.read_csv(f'{file_name}', encoding='utf-8')
            # print('-----------------------------------')
            # print('1. 속성값 모두 동일 여부 검사')
            # print('검사중...')
            # df = num_of_unique_col(df)
            # print(df.head())

            # print('-----------------------------------')
            # print('2. 속성의 distinct 값 비율이 80% 이하인 속성 검사')
            # print('검사중...')
            # prop_df = check_distinct_prop(df, 0.8)
            # print(prop_df)

            # print('-----------------------------------')
            # print('3. 속성의 NULL 을 제외한 값의 비율을 확인')
            # print('검사중...')
            # print(check_not_null_prop(df))

            print('-----------------------------------')
            print('속성의 NULL 비율이 임계치 이상인 행 삭제')
            prop = float(input('임계치를 지정해주세요.(ex. 80%면 0.8입력): '))
            print('검사중...')

            check_null_prop_delete(file_name, prop, chunksize)

            three = three + '(완료)'
            continue
        elif input_num == '4':
            special_char = special_char_list(special_char)

        elif input_num == '5':
            reserved_words = reserve_word_list(reserved_words)

        elif input_num == '6':
            print('종료합니다')
            exit()
