# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    heading = data.find('span', {'id': 'By_market_capitalization'})
    table = heading.find_next('table')
    rows = table.find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) >= 3: 
            data_dict = {"Name": col[1].text.strip(),
                         "MC_USD_Billion": float(col[2].text.strip().replace(',', '').rstrip(']\n'))}
            df1 = pd.DataFrame(data_dict, index=[0])
            if not df1.empty:  # Check if df1 contains any data
                df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df, csv_path):
    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate_dict = exchange_rate_df.set_index('Currency')['Rate'].to_dict()

    # Add columns for transformed market capitalization to respective currencies
    df['MC_GBP_Billion'] = df['MC_USD_Billion'] * exchange_rate_dict.get('GBP')
    df['MC_EUR_Billion'] = df['MC_USD_Billion'] * exchange_rate_dict.get('EUR')
    df['MC_INR_Billion'] = df['MC_USD_Billion'] * exchange_rate_dict.get('INR')

    return df

def load_to_csv(df, output_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

log_progress('Preliminaries complete. Initiating ETL process')


df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')


df = transform(df, "exchange_rate.csv")

log_progress('Data transformation complete. Initiating loading process')


load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')


sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')


query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
log_progress('Server Connection closed')