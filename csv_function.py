import re
from tqdm import tqdm
import numpy as np
import pandas as pd
import os
# 2_physical_instructor_practice_info.csv
def change_col_type(df):
    col_type = ['string', 'integer', 'float']
    for i, col_type_ in enumerate(df.iloc[0]):
        if col_type_ in col_type:
            continue
        else:
            print("---------------------------------")
            print("잘못된 타입이 들어왔습니다.")
            print('col number:', i, ', column type:' ,col_type_)
            print("수정 후 다시 실행해주세요.")
            os.system("pause")
            exit()
    return df



def change_col_special_char(df,patterns):
    #컬럼명에 특수문자, 공백이 있는지 검사 후 값 변경
    df_col = list(df.columns)
    pattern = ''.join(patterns)
    df_col2 = list(map(lambda x: re.sub('_+', '_', re.sub(f'[{pattern}]', '_', x)) if isinstance(x, str) else x, df_col))
    if all(item in df_col2 for item in df_col) and all(item in df_col for item in df_col2):
        print('-----------------------------------')
        print('이상이 없습니다.')
        return df
    else:
        print('-----------------------------------')
        print("특수문자가 속성명에 있습니다. ")
        print("수정 후 다시 실행해주세요.")
        os.system("pause")
        exit()

def change_col_reserved_words(df, patterns):
    column_number = 0
    new_columns = []
    for column in df.columns:
        if column in patterns:
            new_columns.append(f'modified_col_{column_number}')
            column_number += 1
        else:
            new_columns.append(column)
    if all(item in list(df.columns) for item in new_columns) and all(item in new_columns for item in list(df.columns)):
        print('-----------------------------------')
        print('이상이 없습니다.')
        return df
    else:
        print('-----------------------------------')
        print("예약어가 속성명에 포함되어있습니다. ")
        print("수정 후 다시 실행해주세요.")
        os.system("pause")
        exit()


def change_col_start_num(df):
    df_col = list(df.columns)
    num_pattern = r"^\d"
    regex = re.compile(num_pattern)
    check = False
    # Iterate over the list of columns
    for columns in df_col:
    # Use the search function to check if the columns starts with a number
        match = regex.search(columns)
        if match:
        # Use the sub function to replace the columns with 'col'
            modified_column = regex.sub('_', columns)
            df.rename(columns = {columns:modified_column},inplace=True)
            check = True
        else: continue
    if check == True:
        print('-----------------------------------')
        print("숫자로 시작하는 속성이 있습니다. ")
        print("수정 후 다시 실행해주세요.")
        os.system("pause")
        exit()
    else:
        print('-----------------------------------')
        print('이상이 없습니다.')
        return df



def change_word_null(df, patterns):
    # Create the regular expression pattern
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)
    # Use the applymap() method to apply the re.sub() function to each element in the DataFrame
    result = df.applymap(lambda x: re.sub(pattern, '', x) if isinstance(x, str) else x)
    return result

def change_word_special_char(df, patterns):
    # Create the regular expression pattern
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)

    # print(df2)
    col_type = ['integer', 'float']
    numeric_col_index = []

    for i, col_type_ in enumerate(df.iloc[0]):
        if col_type_ in col_type:
            numeric_col_index.append(i)
        else:
            continue
    # print(numeric_col_index)
    if len(numeric_col_index) == 0:
        # print('there are none numeric type columns')
        return df
    else:  df.iloc[:, numeric_col_index] = df.iloc[:, numeric_col_index].applymap(lambda x: re.sub(pattern, '', x) if isinstance(x, str) else x)

    return df

def chunck_progress_bar(filename, func, patterns, chunksize):
    # check directory
    print('-----------------------------------')
    print('현재 디렉토리 주소는 다음과 같습니다. 변경하시겠습니까?(Y/N)')
    print(os.getcwd())
    while True:
        user_input = input()
        if user_input.lower() == 'y' or user_input.lower() == 'yes':
            new_path = input("새로운 작업 디렉토리를 입력하세요: ")
            os.chdir(new_path)
            print("현재 작업 디렉토리는:", os.getcwd())
            break
        elif user_input.lower() == 'n' or user_input.lower() == 'no':
            break
        else:
            print("다시 입력해주십시오")
    # Define the output file
    print('-----------------------------------')
    file_name = input('저장하고 싶은 파일명을 입력해주세요: ')
    file_name = file_name + '.csv'
    output_file = os.path.join(os.getcwd(), file_name)

    # Initialize the progress bar
    pbar = tqdm(total=sum(1 for row in open(f'{filename}', 'r', encoding='utf-8')))

    # Read and process the CSV file chunk by chunk
    for i, chunk in enumerate(pd.read_csv(f'{filename}', chunksize=chunksize, encoding='utf-8')):
        chunk = func(chunk, patterns)
        # Save the chunk to a new CSV file
        if i == 0:
            # Write the header for the first chunk
            chunk.to_csv(output_file, mode='w', index=False)
        else:
            # Append without writing the header for the subsequent chunks
            chunk.to_csv(output_file, mode='a', index=False, header=False)

        # Update the progress bar
        pbar.update(chunksize)

    # Close the progress bar
    pbar.close()




# 속성 검사
# 1. 속성이 모두 동일한지 확인 후 컬럼 제거
def num_of_unique_col(df):
    distinct_col_index = []
    for index, num_of_col in enumerate(df[1:].nunique()):
        if num_of_col == 0 or num_of_col == 1:
            distinct_col_index.append(index)
        else: continue

    if not distinct_col_index:
        return df
    else:
        print('-----------------------------------')
        print("속성값이 한개만 존재합니다. 해당 속성을 지우시겠습니까?(Y/N)")
        while True:
            user_input = input()
            if user_input.lower() == 'y' or user_input.lower() == 'yes':
                df = df.drop(df.columns[distinct_col_index], axis=1)
                return df
            elif user_input.lower() == 'n' or user_input.lower() == 'no':
                return df
            else:
                print("다시 입력해주십시오")

# 2. 속성의 distinct 값 비율이 prop% 이상이면 True 반환 단, 중복 컬럼명이 없어야함
def check_distinct_prop(df, prop):
    distinct_prop = []
    for col_ in df.columns:
        if df[col_].nunique()/len(df) >= prop:
            distinct_prop.append('이상')
        else :  distinct_prop.append('이하')

    data = []
    for i, num_of_col in enumerate(distinct_prop):
        new_data = {'col': df.columns[i], 'distinct_prop': num_of_col}
        data.append(new_data)

    df = pd.DataFrame(data)
    return df

# 3. NULL 을 제외한 값의 비율을 확인
def check_not_null_prop(df):
    return 1 - df.isnull().mean()

def check_null_prop_delete(filename, prop, chunksize):
    # check directory
    print('-----------------------------------')
    print('현재 디렉토리 주소는 다음과 같습니다. 변경하시겠습니까?(Y/N)')
    print(os.getcwd())
    while True:
        user_input = input()
        if user_input.lower() == 'y' or user_input.lower() == 'yes':
            new_path = input("새로운 작업 디렉토리를 입력하세요: ")
            os.chdir(new_path)
            print("현재 작업 디렉토리는:", os.getcwd())
            break
        elif user_input.lower() == 'n' or user_input.lower() == 'no':
            break
        else:
            print("다시 입력해주십시오")
    # Define the output file
    print('-----------------------------------')
    file_name = input('저장하고 싶은 파일명을 입력해주세요: ')
    file_name = file_name + '.csv'
    output_file = os.path.join(os.getcwd(), file_name)


    # Initialize counters for total entries and total nulls
    total_entries = None
    total_nulls = None


    print('Null 비율 계산 중...')
    # Initialize the progress bar
    pbar = tqdm(total=sum(1 for row in open(f'{filename}', 'r', encoding='utf-8')))
    # Read and process the CSV file chunk by chunk
    for i, chunk in enumerate(pd.read_csv(f'{filename}', chunksize=chunksize, encoding='utf-8')):

        # For the first chunk, initialize the counters with its shape and null counts
        if total_entries is None:
            total_entries = pd.Series(np.zeros(chunk.shape[1]), index=chunk.columns)
            total_nulls = chunk.isnull().sum()

        # For subsequent chunks, increment the counters
        else:
            total_entries += chunk.shape[0]
            total_nulls += chunk.isnull().sum()

        # Update the progress bar
        pbar.update(chunk.shape[0])

    pbar.close()
    # Calculate the null proportions
    null_proportions = total_nulls / total_entries

    # Find the columns to drop
    cols_to_drop = null_proportions[null_proportions > prop].index

    # Close the progress bar

    # print(cols_to_drop)
    # print(null_proportions)

    # Prepare an empty DataFrame for storing the processed chunks
    # print(len(cols_to_drop))

    if len(cols_to_drop) == 0:
        print('모든 열의 NULL값이 임계치 미만입니다.')
    else:
        # Initialize the progress bar
        pbar = tqdm(total=sum(1 for row in open(f'{filename}', 'r', encoding='utf-8')))

        print('해당 열 삭제 중...')
        # Read and process the CSV file chunk by chunk again
        for i, chunk in enumerate(pd.read_csv(f'{filename}', chunksize=chunksize, encoding='utf-8')):
            chunk = chunk.drop(columns=cols_to_drop)
            if i == 0:
                # Write the header for the first chunk
                chunk.to_csv(output_file, mode='w', index=False)
            else:
                # Append without writing the header for the subsequent chunks
                chunk.to_csv(output_file, mode='a', index=False, header=False)
            # Update the progress bar
            pbar.update(chunk.shape[0])

        # Close the progress bar
        pbar.close()



def special_char_list(special_char, sql_special_char):
    print("-----------------------------------")
    print(f'검사할 특수문자는 다음과 같습니다. {sql_special_char}')
    try:
        loop_manager = True
        while loop_manager:
            print('번호를 입력하세요.')
            print('1. 특수문자 추가')
            print('2. 특수문자 제외')
            print('3. 종료')
            input_number = input()
            if input_number == '1':
                first_loop = True
                while first_loop:
                    print('한 문자씩 입력바랍니다. 종료를 원할시 exit을 입력해주세요.')
                    while True:
                        new_char = input('특수문자 입력(or exit) : ')
                        if new_char == 'exit':
                            first_loop = False
                            break
                        else:
                            # column_patterns.append(new_char)
                            special_char.append('\\'+new_char)
                            sql_special_char.append(new_char)
                            print('추가되었습니다.')
                            print(sql_special_char)

            elif input_number == '2':
                first_loop = True
                while first_loop:
                    try:
                        print('한 문자씩 입력바랍니다. 종료를 원할시 exit을 입력해주세요.(%를 지우기 위해서는 %%, % 모두 입력해야 합니다)')
                        while True:
                            new_char = input('특수문자 입력(or exit) : ')
                            if new_char == 'exit':
                                first_loop = False
                                break
                            else:
                                try:
                                    sql_special_char.remove(new_char)
                                except:
                                    pass
                                try:
                                    special_char.remove('\\'+new_char)
                                except:
                                    pass
                                try:
                                    special_char.remove(new_char)
                                except:
                                    pass
                                print('제거되었습니다.')
                                print(sql_special_char)
                    except Exception as e:
                        print(f'error: ', e)
            elif input_number == '3':
                loop_manager = False
                break
            else:
                print('다시 입력해주십시오.')
    except Exception as e:
        print('error :', e)

    return special_char, sql_special_char


def reserve_word_list(reserved_words):
    print("-----------------------------------")
    print(f'검사할 예약어는 다음과 같습니다. {reserved_words}')
    try:
        loop_manager = True
        while loop_manager:
            print('번호를 입력하세요.')
            print('1. 예약어 추가')
            print('2. 예약어 제외')
            print('3. 종료')
            input_number = input()
            if input_number == '1':
                first_loop = True
                while first_loop:
                    print('한 단어씩 입력바랍니다. 종료를 원할시 exit을 입력해주세요.')
                    while True:
                        new_char = input('예약어 입력 : ')
                        if new_char == 'exit':
                            first_loop = False
                            break
                        else:
                            reserved_words.append(new_char)
                            print('추가되었습니다.')
                            print(reserved_words)

            elif input_number == '2':
                first_loop = True
                while first_loop:
                    try:
                        print('한 단어씩 입력바랍니다. 종료를 원할시 exit을 입력해주세요.')
                        while True:
                            new_char = input('예약어 입력 : ')
                            if new_char == 'exit':
                                first_loop = False
                                break
                            else:
                                reserved_words.remove(new_char)
                                print('제거되었습니다.')
                                print(reserved_words)
                    except Exception as e:
                        print(f'error: ', e)
            elif input_number == '3':
                loop_manager = False
                break
            else:
                print('다시 입력해주십시오.')
    except Exception as e:
        print('error :', e)

    return reserved_words