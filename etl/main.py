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
import pandas as pd
import pickle as pkl
import configparser 
from sqlalchemy import create_engine
from utils.util import rename_columns, date_formatting, remove_prefixes,\
                        remove_duplicates, capitalize, handle_misspslled,\
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
    
    # reads the airlines pickle file 
    with open(airlines_path, 'rb') as file:
        airlines_list = pkl.load(file)
    
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
    
    # Handling missing values in airlines column
    df['Airline'] = handle_misspslled(df['Airline'], airlines_list)

    return df.copy()


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

    airlines_path = config['path']['airlines_path']

    data_file_path = config['path']['data_file_path']

    host = config['mysql']['host']

    username = config['mysql']['user']

    password = config['mysql']['password']

    database = config['mysql']['database']

    table_name = config['mysql']['table_name']


    # reads the data file using pandas
    df = pd.read_csv(data_file_path)

    # performing Transformations
    transformed_df = perform_transformations(df, airlines_path, date_columns)

    # creating connection to mysql database using mysqlalchemy module
    connection = create_engine("mysql+pymysql://" + username + ":" + password + "@" + host + "/" + database)

    # writing the transformed data to the target database
    write_to_database(transformed_df, connection, table_name, date_columns )

    # disposes the database connection 
    connection.dispose()






if __name__ == '__main__':
    
    # Executing main method
    main()
