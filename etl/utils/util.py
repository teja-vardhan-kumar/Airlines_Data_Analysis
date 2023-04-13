"""
Description : This file provides methods to transform the data

Author : P. Teja Vardhan Kumar
Created Date : 13-04-2023

"""

# import statements
import pandas as pd
from difflib import get_close_matches



def rename_columns(df):
    """ Parameters: df (dataframe) : pandas dataframe of dataset
        Returns: Series : modified pandas series object
    """
    return df.columns.str.strip().str.replace(' ', '_')

def date_formatting(column):
    """ Parameters: column (Series) :  pandas series object
        Returns: Series : modified pandas series object
    """
    
    return pd.to_datetime(column, format='mixed' ).dt.strftime('%d-%m-%Y')

def remove_prefixes(column):
    """ Parameters: column (Series) :  pandas series object
        Returns: Series : modified pandas series object
    """

    return column.str.replace('[MR | MRS]\.?\s*', '', case=False, regex = True)

def remove_duplicates(column):
    """ Parameters: column (Series) :  pandas series object
        Returns: Series : modified pandas series object
    """
    
    return column.str.replace(r'([^/]+)/\1', r'\1', regex = True)

def capitalize(column):
    """ Parameters: column (Series) :  pandas series object
        Returns: Series : modified pandas series object
    """
    
    return column.str.capitalize()

# This function replace the misspelled values 
def replace_with_closest_match(name, airlines_array):
    
    if name.lower() in airlines_array:
        return name
    else:
        closest_match = get_close_matches(name.lower(), airlines_array, n=1, cutoff=0.6)
        if closest_match:
            return closest_match[0].capitalize()
        else:
            return name

def handle_misspelled(column, airlines_array):
    """ Parameters: column (Series) :  pandas series object
                    airlines_list (list) : airlines names list 
        Returns: Series : modified pandas series object
    """

    return column.apply(replace_with_closest_match, airlines_array = airlines_array)

def write_to_database(df, conn, table_name, date_columns):
    """ Parameters: df (dataframe) :  pandas dataframe
                    conn : database connection object
                    table_name(string) : name of the target table
                    date_cloumns (list) : list of date column names
    """

    for column in date_columns:
        df[column] = pd.to_datetime(df[column]).dt.strftime('%Y-%m-%d')

    df.to_sql(table_name, con = conn, if_exists='append', index = False,\
              chunksize=1000, method = 'multi' )