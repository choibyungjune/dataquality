import pandas as pd
import mariadb
import pymysql
from sqlalchemy import create_engine
import os
import re
import glob


# The words we want to find
words = ['"', "'", '{', '}','\n']

# The DataFrame of strings
df = pd.DataFrame([[1, "'2", 3.0], [4, None, '\n'], [7, 8, 9], [None, None, None]])

# Define a function that searches for the words
def find_words(x):
    # Convert the float value to a string
    x = str(x)
    # Escape the special characters in the words list
    # words_escaped = [re.escape(w) for w in words]
    # print(words_escaped)
    # Create the regular expression pattern
    pattern = re.compile('|'.join(words_escaped))
    # Find all occurrences of the words in the string
    matches = pattern.findall(x)
    return matches

# Use the apply() method to apply the function to each element in the DataFrame
print(df.apply(find_words))
exit()
print(df)
exit()
# # df = pd.DataFrame([[1, "\'2", 3], [4, None, '\n'], [7, 8, 9], [None, None, None]])

# # The words we want to find
# words = [' ', '"', "'", '{', '}','\n']

# # The DataFrame of strings
# df = pd.DataFrame([[1, "\'2", ' '], [4, None, '\n'], [7, 8, 9], [None, None, None]])

# # Define a function that searches for the words
# def find_words(text):
#     pattern = re.compile('|'.join(words))
#     matches = pattern.findall(text)
#     return matches

# print(df.apply(find_words))


# exit()

special_character = ['\"', "\'", '{', '}','\n']
print(special_character)
# exit()

mask = df.applymap(lambda x: bool(x.str.contains(r'^(?=')) 
                                  & set(special_character))

print(set(special_character))
print(set(str(df)))
print(mask)
print(mask.sum().sum()) #행, 열 합계 구하기

print("111")
exit()

def check_special_characters(df): # checking single character
    # Create a boolean mask to identify cells that contain special characters

    #mask = df.applymap(lambda x: bool(set(str(x)) & set('[0-9+]')))
    special_character = ['\"', "\'", '{', '}','\n']
    # Use the mask to count the number of special characters in each cell
    mask = df.applymap(lambda x: bool(set(str(x)) & set(special_character)))
    count = mask.sum().sum()

    return count

def check_special_characters(df): # checking single character
    # Create a boolean mask to identify cells that contain special characters

    mask = df.applymap(lambda x: bool(set(str(x)) & set('[0-9+]')))
    special_character = ['\"', "\'", '{', '}','\n']
    # Use the mask to count the number of special characters in each cell
    mask = df.applymap(lambda x: bool(set(str(x)) & set(special_character)))
    count = mask.sum().sum()

    return count



txtfiles = []
for file in glob.glob("*.csv"):
    txtfiles.append(file)

print(txtfiles)

for i in txtfiles :
    
    # df = pd.read_csv("C:/Users/Yonsei/Downloads/소상공인시장진흥공단_상가(상권)정보_20220930/soho_seoul.csv")
    df = pd.read_csv(f"./{i}")
    
    # 여기서 속성명 속성값 검사
    # print(df.columns)
    count = check_special_characters(df)
    print(count)
    
    print("111")
    exit()

    
    
    
    