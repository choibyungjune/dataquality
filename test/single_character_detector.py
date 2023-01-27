import pandas as pd
import mariadb
import pymysql
from sqlalchemy import create_engine
import os
import re
import glob
# The DataFrame of strings
df = pd.DataFrame([[1, "'2", 3.0], [4, None, '\n'], [7, 8, 9], [None, None, None]])

def check_special_characters(df): # checking single character
    # Create a boolean mask to identify cells that contain special characters

    #mask = df.applymap(lambda x: bool(set(str(x)) & set('[0-9+]')))
    special_character = ['\"', "\'", '{', '}','\n']
    # Use the mask to count the number of special characters in each cell
    mask = df.applymap(lambda x: bool(set(str(x)) & set(special_character)))
    count = mask.sum().sum()

    return count


count = check_special_characters(df)
print(df)
print(count)



