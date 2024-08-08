# Code for ETL operations on Country-GDP data
#hector de leon 2024
# The following code extracts, transforms and loads data about the top 10 banks in the world into a database.
# The code also makes possible to write queries to extract specific information about the database.
# Each part of the process is done in a separate function, and each function is executed at the end.
# the proces is recorded into a .txt file by using the log_progress function. 

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = 'exchange_rate.csv'
output_path = './Largest_banks_data.csv'
# Importing the required libraries

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            #column 1 has 2 hyperlinks; we need the name asociated the second one
            if col[1].find_all('a') is not None:
                links = col[1].find_all('a') #finds all links within the row's 2nd column
                col2contents = col[2].contents[0].rstrip('\n')
                data_dict = {"Name": links[1].contents[0],#acces the info asociated with link 2
                            "MC_USD_Billion": col2contents}
                #print(data_dict)
                df1 = pd.DataFrame(data_dict, index=[0])
                #print(df1)
                df = pd.concat([df,df1], ignore_index=True)
                df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)

            else:
                print('none')

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Initializing ETL process')

df = extract(url, table_attribs)
#print(df)
log_progress('Extraction complete, starting transform process')

df = transform(df, csv_path)
#print(df)
#market capitalization for the 5th largest bank in billion EUR: 146.86
log_progress('Transform process complete, starting load to csv process')

load_to_csv(df,output_path)

log_progress('Load to csv complete, starting load to database')

sql_connection = sqlite3.connect('Banks.db')
load_to_db(df,sql_connection,table_name)

log_progress('Load to database complete, starting Queries')

query_statement = f'SELECT * FROM {table_name}'
run_query(query_statement,sql_connection)


query_statement = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
run_query(query_statement,sql_connection)


query_statement = f'SELECT Name FROM {table_name} LIMIT 5'
run_query(query_statement,sql_connection)

log_progress('Queries complete, process complete.')

sql_connection.close()


