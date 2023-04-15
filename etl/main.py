"""
Description : This file performs below tasks
             1. Loads the dataset from local environmnet
             2. parses config.ini file for required configuration details
             3. Performs required transformation on the dataset using 
                perform_transformations method
             4. creates connection to mysql database
             5. Loads the transformed data into the target database 

Author : P. Teja Vardhan Kumar
Created Date : 13-04-2023

"""

# import statements
import sys
import json
import numpy as np
import pandas as pd
import pickle as pkl
import configparser 

from sqlalchemy import create_engine
from utils.util import rename_columns, date_formatting, remove_prefixes,\
                        remove_duplicates, capitalize, handle_misspelled,\
                        write_to_database


def perform_transformations(df, airlines_path, date_columns):
    
    """
    This function Performs required trasformation on dataset

    Parameters: 
    df (dataframe) : pandas dataframe of dataset
    airlines_path : path for the airline names pickle file
    date_columns : date column names in the dataset

    Returns:
    dataframe : copy of a transformed dataframe

    """
    
    # renaming the columns
    df.columns = rename_columns(df)

    # Formatting the date columns
    for date_column in date_columns:
        df[date_column] = date_formatting(df[date_column])

    # Removing the prefixes from Pax_Name column
    df['Pax_Name'] = remove_prefixes(df['Pax_Name'])

    # Removing duplicates from the Airline column
    df['Airline'] = remove_duplicates(df['Airline'])

    # Capitalizing all columns
    for column in df.columns:
        if column not in date_columns:
            df[column] = capitalize(df[column])
    
    # deserializing the airlines pickle file and storing in numpy array
    with open(airlines_path, 'rb') as file:
        airlines_array = np.array(pkl.load(file))

    # Handling missing values in airlines column
    df['Airline'] = handle_misspelled(df['Airline'], airlines_array)

    return df.copy()


def update_airlines():

    """
    This function updates realtime airline names present in airlines json file 
    into airlines pickle file, so that storing airline names is optimized and 
    well maintained

    """
    # creating parser object
    config = configparser.ConfigParser()

    # Reading the config file
    config.read('config.ini')

    airlines_pickle_path = config['path']['airlines_pickle_path']

    airlines_json_path = config['path']['airlines_json_path']

    # Read the realtime airlines json file
    with open(airlines_json_path, 'r') as json_file:
        json_data = json.load(json_file)['airlines']

    # Serializing airlines names into pickle format to store efficiently
    with open(airlines_pickle_path, 'wb') as pickle_file:
        pkl.dump(json_data, pickle_file)
    

    

def main():

    """
    This function is main interface for the project. It parses the config file,
    creates connection to database and loads transformed data into database

    """
    # creating parser object
    config = configparser.ConfigParser()

    # Reading the config file
    config.read('config.ini')

    date_columns = ['Booking_Date', 'Travel_Date']

    airlines_path = config['path']['airlines_pickle_path']

    dataset_path = config['path']['dataset_path']

    host = config['mysql']['host']

    username = config['mysql']['user']

    password = config['mysql']['password']

    database = config['mysql']['database']

    table_name = config['mysql']['table_name']


    # reads the data file using pandas
    df = pd.read_csv(dataset_path)

    # performing Transformations
    transformed_df = perform_transformations(df, airlines_path, date_columns)

    # creating connection to mysql database using mysqlalchemy module
    connection = create_engine("mysql+pymysql://" + username + ":" + password + "@" + host + "/" + database)

    # writing the transformed data to the target database
    write_to_database(transformed_df, connection, table_name, date_columns )

    # disposing the database connection 
    connection.dispose()






if __name__ == '__main__':

    # checking for command line argumenst
    if sys.argv[1:2] == ['update']:

        # performing updation on airlines names
        update_airlines()
    
    # Executing main method
    main()
