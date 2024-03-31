import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import random
import os
from selenium.webdriver.common.by import By
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import logging
import yaml
from selenium.webdriver.common.action_chains import ActionChains

logging.basicConfig(
    level=logging.DEBUG,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log format
    filename='app.log',  # Specify the log file name
    filemode='w'  # Choose the file mode ('w' for write, 'a' for append)
)

logger = logging.getLogger()

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)


pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

with open('config.yaml', 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

download_folder = config['download_folder']
extract_files_count = config['extract_files_count']
file_filter = config['file_filter']
data_filters = config['data_filters']
db_host = config['db_host']
db_port = config['db_port']
db_name = config['db_name']
db_user = config['db_user']
db_user_pwd = config['db_user_pwd']
table_name = config['table_name']
row_count_per_file = config['row_count_per_file']
chunk_size = config['chunk_size']

engine = create_engine(f'postgresql://{db_user}:{db_user_pwd}@{db_host}:{db_port}/{db_name}')


def generate_sql_table(df, table_name='tb_testing'):
    data_types = df.dtypes
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for column_name, data_type in data_types.items():
        sql_type = ""
        if data_type == 'object':
            sql_type = 'VARCHAR(255)'
        elif 'int' in str(data_type):
            sql_type = 'INT'
        elif 'float' in str(data_type):
            sql_type = 'FLOAT'
        elif 'datetime' in str(data_type):
            sql_type = 'TIMESTAMP'
        else:
            sql_type = 'VARCHAR(255)'

        create_table_sql += f"\n\t{column_name} {sql_type},"
    create_table_sql = create_table_sql.rstrip(',')
    create_table_sql += "\n);"
    return create_table_sql


def transform():
    parquet_files = [file for file in os.listdir(os.path.join(os.getcwd(),  download_folder)) if file.endswith('.parquet')]
    data_frames = []
    for file in parquet_files:
        file_path = os.path.join(os.path.join(os.getcwd(),  download_folder), file)
        df = pd.read_parquet(file_path)
        data_frames.append(df)
        logger.info(f'Reading {file}.')
    combined_df = pd.concat(data_frames, ignore_index=True)
    mask = combined_df.eval(' & '.join(data_filters))
    filtered_data = combined_df[mask]

    if row_count_per_file > 0:
        filtered_data = filtered_data.head(row_count_per_file * extract_files_count)

    # filtered_data.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi')

    total_chunks = (len(filtered_data) - 1) // chunk_size + 1

    chunks = [filtered_data[i * chunk_size:(i + 1) * chunk_size] for i in range(total_chunks)]

    for idx, chunk_df in enumerate(chunks):
        chunk_df.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi')
        logger.info(f'Loading {idx} of {total_chunks} chunks.')
    return filtered_data


def extract():
    chrome_options = Options()

    download_dir = os.path.join(os.getcwd(),  download_folder)
    prefs = {"download.default_directory": download_dir}
    chrome_options.add_experimental_option("prefs", prefs)
    with open(f"user-agents.txt", "r+") as file:
        file_contents = file.read().split("\n")
    random.shuffle(file_contents)
    ua = file_contents[0]
    chrome_options.add_argument(f"--user-agent={ua}")
    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()
    driver.set_window_size(1920, 1080)
    # actions = ActionChains(driver)

    driver.get("https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
    a_tags = driver.find_elements(By.TAG_NAME, "a")

    files_counter = 0

    for a_tag in a_tags:
        href = a_tag.get_attribute("href")
        text = a_tag.text
        if '.parquet' in href and file_filter in href:
            response = requests.get(href)
            filename = os.path.join(download_folder, f'{href.split("/")[-1]}')
            logger.info(f'Extracting {filename}.')
            with open(filename, "wb") as file:
                file.write(response.content)
            files_counter += 1

        if files_counter >= extract_files_count:
            break


    time.sleep(10)
    driver.quit()


def aggregates_and_visualize():
    query = f"SELECT tpep_pickup_datetime, total_amount FROM {table_name}"
    df = pd.read_sql(query, con=engine)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df = df[df['tpep_pickup_datetime'].dt.year == 2024]
    daily_total = df.groupby(df['tpep_pickup_datetime'].dt.date)['total_amount'].sum().reset_index()
    plt.figure(figsize=(10, 6))
    plt.plot(daily_total['tpep_pickup_datetime'], daily_total['total_amount'], marker='o')
    plt.xlabel('Date')
    plt.ylabel('Total Amount')
    plt.title('Total Amount per Day')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':

    extract()
    transform()
    aggregates_and_visualize()

