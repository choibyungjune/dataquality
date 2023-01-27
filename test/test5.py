import pandas as pd
import re

# The DataFrame of strings
df = pd.DataFrame([[1, "'2", 'select'], [4, None, '\n'], [7, ' ', 9], [None, None, None]], columns=[' ', '2', 'where'])

# The patterns to replace
patterns = ['"', "'", '{', '}','\n', 'select', 'from', 'where', ' ']

df_columns = pd.DataFrame(df.columns)

# def replace_pattern(row, pattern):
#     for i, x in enumerate(row):
#         if isinstance(x, str) and pattern.search(x):
#             row[i] = 'col {}'.format(i+1)
#     return row

# def change_column(df, patterns):
#     # Create the regular expression pattern
#     pattern = '|'.join(patterns)
#     pattern = re.compile(pattern, flags=re.IGNORECASE)
    
#     df_columns = 
    
    
#     #값 바꾸는 법
#     result = df_columns.apply(lambda row: replace_pattern(row, pattern), axis=1)
#     print(result)
    
#     # Rename the columns
#     # df.columns = ['col {}'.format(i+1) for i in range(len(df.columns)) if df.columns[i]  ]
    
#     return result
# # print(pd.Series(df.columns))
# # print(change_column(df, patterns))


def change_column(df, patterns):
    df_col = list(df.columns)
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)
    for i in range(len(df_col)):
        if isinstance(df_col[i], str) and re.search(pattern,df_col[i]):
            df_col[i] = 'col {}'.format(i+1)
    df.columns = df_col
    return df


print(change_column(df, patterns))
