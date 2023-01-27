import pandas as pd
import re

# The DataFrame of strings
df = pd.DataFrame([[1, "'2", 'select'], [4, None, '\n'], [7, ' ', 9], [None, None, None]], columns=[' ', '2', 'where'])

# The patterns to replace
patterns = ['"', "'", '{', '}','\n', 'select', 'from', 'where', ' ']

def replace_pattern(row):
    for i, x in enumerate(row):
        if isinstance(x, str):
            row[i] = 'col {}'.format(i+1)
    return row

def change_column(df, patterns):
    # Create the regular expression pattern
    pattern = '|'.join(patterns)
    pattern = re.compile(pattern, flags=re.IGNORECASE)
    
    # Rename the columns
    df = df.rename(columns=replace_pattern)
    
    return df

print(change_column(df, patterns))