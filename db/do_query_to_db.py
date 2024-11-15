import yaml
import os
#import subprocess
import psycopg2
import pandas as pd
from psycopg2 import OperationalError
from sqlalchemy import create_engine
import random
import datetime


with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)


LIMIT = config['query_limit']
OFFSET = config['offset']


def create_connection_to_remote_db():
    try:
        connection = psycopg2.connect(
            user=config['remote_db']['user'],
            password=config['remote_db']['password'],
            host=config['remote_db']['host'],
            port=config['remote_db']['port'],
            database=config['remote_db']['database'],
        )
        print("Connection to the REMOTE PostgreSQL DB successful")
        return connection
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        return None
 
def create_connection_to_local_db():
    try:
        connection = psycopg2.connect(
            user=config['local_db']['user'],
            password=config['local_db']['password'],
            host=config['local_db']['host'],
            port=config['local_db']['port'],
            database=config['local_db']['database'],
        )
        print("Connection to the LOCAL PostgreSQL DB successful")
        return connection
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        return None

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(result, columns=columns)

########################################################### MINES

def do_query_to_db():
    conn = create_connection_to_remote_db()
    query = f'SELECT * FROM public."DJ_NEWS_STORIES" \
              LIMIT { LIMIT } OFFSET { OFFSET };'

    df = execute_query(conn, query)
    conn.close()
    return df

def do_query_to_local_db(limit, offset, db):
    conn = create_connection_to_local_db()
    if db == "synthetic":
        query = f'SELECT * FROM public."SYNTHETIC_NEWS_STORIES" \
                LIMIT { limit } OFFSET { offset };'
    elif db == "dj":
        query = f'SELECT * FROM public."DJ_NEWS_STORIES" \
                LIMIT { limit } OFFSET { offset };'

    df = execute_query(conn, query)
    conn.close()
    return df

def do_query_count_mines_in_remote_db():
    conn = create_connection_to_remote_db()
    query = f'SELECT COUNT(*) FROM public."MINES";'

    count = execute_query(conn, query)
    conn.close()
    num = count['count'][0]
    return num

def do_query_count_mines_in_local_db():
    conn = create_connection_to_local_db()
    query = f'SELECT COUNT(*) FROM "MINES";'

    count = execute_query(conn, query)
    conn.close()
    num = count['count'][0]
    return num

def create_mines_table_in_local_db():
    conn = create_connection_to_local_db()
    if conn is not None:
        try:
            query = '''
            CREATE TABLE IF NOT EXISTS "MINES" (
                "PROP_NAME" VARCHAR(255),
                "PROP_ID" INTEGER,
                "PRIMARY_COMMODITY" VARCHAR(255),
                "ACTV_STATUS" VARCHAR(255),
                "MINE_TYPE1" VARCHAR(255),
                "COMMODITIES_LIST" VARCHAR(255),
                "OWNER_NAME" VARCHAR(255),
                "SNL_GLOBAL_REGION" VARCHAR(255),
                "COUNTRY_NAME" VARCHAR(255),
                "STATE_PROVINCE" VARCHAR(255),
                "LATITUDE" VARCHAR(255),
                "LONGITUDE" VARCHAR(255)
            );
            '''
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            cursor.close()
            print("Table MINES has been created")
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
        finally:
            conn.close()
    else:
        print("Failed to create a connection to the local database")
    
def do_query_all_mines_in_remote_db():
    conn = create_connection_to_remote_db()
    query = f'SELECT * FROM public."MINES" ;'

    df = execute_query(conn, query)
    conn.close()
    return df

def do_query_uniq_mines_property_and_owners_local_db():
    conn = create_connection_to_local_db()
    query = f'SELECT DISTINCT "PROP_NAME", "OWNER_NAME" \
              FROM public."MINES" ORDER BY "PROP_NAME" ASC;'

    df = execute_query(conn, query)
    conn.close()

    owner_name_list = [name for name in df['OWNER_NAME'].tolist() if name is not None]
    prop_name_list  = [name for name in df['PROP_NAME'].tolist() if name is not None]

    total_list = prop_name_list + owner_name_list

    unique_sorted_list = set(total_list)
    return sorted(unique_sorted_list)

def do_query_update_local_db_mines():
    try:
        mines_in_remote_db = do_query_all_mines_in_remote_db()
        username = str(config['local_db']['user'])
        password = str(config['local_db']['password'])
        host = str(config['local_db']['host'])
        port = str(config['local_db']['port'])
        database = str(config['local_db']['database'])
        engine = create_engine('postgresql://' + username + ':' + password + '@' \
                               + host + ':' + port + '/' + database)
        mines_in_remote_db.to_sql(
            name="MINES",
            con=engine,
            if_exists="append",
            index=False
        )
        print(f"MINES table in LOACAL DB updated")
    except psycopg2.Error as e:
        print(f"Error updating MINES table: {e}")

def do_query_insert_local_db_mines():
    conn = create_connection_to_local_db()
    if conn is not None:
        try:
            query = '''
                INSERT INTO public."MINES" ("OWNER_NAME", "PROP_NAME")
                 VALUES ('Indig Acquires', 'Phreesia Inc');
                '''
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            cursor.close()
            print("the record has been inserted")
        except psycopg2.Error as e:
            print(f"Error insetring table: {e}")
        finally:
            conn.close()
    else:
        print("Failed to create a connection to the local database")


########################################################### NEwS


def do_query_count_news_in_remote_db():
    conn = create_connection_to_remote_db()
    query = f'SELECT COUNT(*) FROM public."DJ_NEWS_STORIES";'

    count = execute_query(conn, query)
    conn.close()
    num = count['count'][0]
    return num

def do_query_count_news_in_local_db():
    conn = create_connection_to_local_db()
    query = f'SELECT COUNT(*) FROM "DJ_NEWS_STORIES";'

    count = execute_query(conn, query)
    conn.close()
    num = count['count'][0]
    return num

def create_synthetic_news_table_in_local_db():
    conn = create_connection_to_local_db()
    if conn is not None:
        try:
            query = '''
            CREATE TABLE IF NOT EXISTS "SYNTHETIC_NEWS_STORIES" (\
              "ISIN" VARCHAR,\
              "date" TIMESTAMP,\
              "story" TEXT,\
              "id" INTEGER,\
              "ner_results" TEXT,\
              "lang" VARCHAR(10),\
              "description" VARCHAR);
            '''
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            cursor.close()
            print("Table SYNTHETIC_NEWS_STORIES has been created")
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
        finally:
            conn.close()
    else:
        print("Failed to create a connection to the local database")


def insert_data_from_csv_to_local_db():

    file_path = "./files/synthetic_news_set.txt"

    # Reading the file and storing the lines in a list
    with open(file_path, "r") as file:
        corpus = [line.strip() for line in file.readlines() if line.strip()]
    

    conn = create_connection_to_local_db()
    cursor = conn.cursor()

    cursor.execute('SELECT COALESCE(MAX(id), 0) FROM public."SYNTHETIC_NEWS_STORIES";')
    max_id = cursor.fetchone()[0]

    for i, story in enumerate(corpus, start=max_id + 1):
        ISIN = ''.join(random.choices('0123456789', k=6))
        date = datetime.date.today()
        query = '''
        INSERT INTO public."SYNTHETIC_NEWS_STORIES" ("id", "story", "ISIN", "date")
        VALUES (%s, %s, %s, %s);
        '''
        cursor.execute(query, (i, story, ISIN, date))
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully.")

def do_query_all_news_in_remote_db():

    conn = create_connection_to_remote_db()
    query = f'SELECT * FROM public."DJ_NEWS_STORIES" \
              LIMIT { LIMIT } OFFSET { OFFSET };'

    df = execute_query(conn, query)
    conn.close()
    return df

def do_query_update_local_db_news():
    try:
        mines_in_remote_db = do_query_all_news_in_remote_db()
        username = str(config['local_db']['user'])
        password = str(config['local_db']['password'])
        host = str(config['local_db']['host'])
        port = str(config['local_db']['port'])
        database = str(config['local_db']['database'])
        engine = create_engine('postgresql://' + username + ':' + password + '@' \
                               + host + ':' + port + '/' + database)
        mines_in_remote_db.to_sql(
            name="DJ_NEWS_STORIES",
            con=engine,
            if_exists="append",
            index=False
        )
        print(f"DJ_NEWS_STORIES table in LOACAL DB updated")
    except psycopg2.Error as e:
        print(f"Error updating MINES table: {e}")

############################################################ ENTITIES

def create_entities_spacy_table_in_local_db():
    conn = create_connection_to_local_db()
    if conn is not None:
        try:
            query = '''
            CREATE TABLE IF NOT EXISTS "ENTITIES_SPACY" (\
              "entity_name" VARCHAR,\
              "entity_type" VARCHAR,\
              "doc_id" INTEGER,\
              "doc_date" TIMESTAMP);
            '''
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            cursor.close()
            print("Table ENTITIES_SPACY has been created")
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
        finally:
            conn.close()
    else:
        print("Failed to create a connection to the local database")


