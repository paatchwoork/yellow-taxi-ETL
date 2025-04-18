from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import itertools
import os

from src.load import exec_sql

if __name__=='__main__' :

    download = False

    year = [y for y in range(2009,2026)]
    month = [m for m in range(1,13)]
    taxi = {1:'yellow', 2:'green'}#, 3:'fhv', 4:'fhvhv'}
    config = {
        'host' : 'localhost',
        'port' : 5432,
        'user' : 'admin',
        'password' : 'root',
        'database' : 'nyc_trip_record'}

    engine = create_engine(f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
 
    if download :
        pass

    try :
        conn = psycopg2.connect(**config)
        conn.autocommit = True
    except psycopg2.Error as e :
        print('Error connecting to the database')
        print(e)
    else :
        print('Successfully connected to database.')

    sql = """DROP TABLE IF EXISTS taxi""" 
    exec_sql(sql,**config)

    sql = """CREATE TABLE taxi(
    type_id INT,
    taxi_type TEXT);""" 
    exec_sql(sql,**config)

    for t in taxi :
        sql = f"""INSERT INTO taxi (type_id, taxi_type)
        VALUES ({t}, '{taxi[t]}')"""
        exec_sql(sql,**config)

    sql = """DROP TABLE IF EXISTS calendar;""" 
    exec_sql(sql,**config)

    sql = """CREATE TABLE calendar(
    calendar_entry_id INT,
    taxi_type_id INT,
    year INT,
    month INT);""" 
    exec_sql(sql,**config)

    ce_id = 1
    for t_id,y,m in itertools.product(taxi,year,month) :
        # Create entry in the calendar table
        sql = f"""INSERT INTO calendar (calendar_entry_id,taxi_type_id, year, month)
        VALUES ({ce_id}, {t_id}, {y}, {m})"""
        exec_sql(sql,**config)

        pq_file = './data/{t}/{y}/{t}_tripdata_{y}-{m:02d}.parquet'.format(t=taxi[t_id],y=y,m=m)
        # print(pq_file)
        if os.path.isfile(pq_file) :
            print(f'Loading {pq_file} into pandas...')
            df = pd.read_parquet(pq_file,engine='fastparquet')
            df = df.assign(calendar_entry_id = ce_id) 
            # print(df.columns.to_list())
            
            # print(df['calendar_entry_id'])
            # print('Creating CSV file...')
            # df.to_csv('./temp/temp.csv', header=False, index=False) 
            # print('Bulk inserting into the database...')

            sql = """DROP TABLE IF EXISTS {t}_tripdata_{y}_{m:02d};""" .format(t=taxi[t_id],y=y,m=m)
            exec_sql(sql,**config)
    
            sql = """CREATE TABLE {t}_tripdata_{y}_{m:02d} 
            (VendorID INT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count FLOAT,
            trip_distance FLOAT,
            RatecodeID FLOAT,
            store_and_fwd_flag CHAR(1),
            PULocationID INT,
            DOLocationID INT,
            payment_type INT,
            fare_amount FLOAT,
            extra FLOAT,
            mta_tax FLOAT,
            tip_amount FLOAT,
            tolls_amount FLOAT,
            improvement_surcharge FLOAT,
            total_amount FLOAT,
            congestion_surcharge FLOAT,
            airport_fee FLOAT,
            calendar_entry_id INT)""".format(t=taxi[t_id],y=y,m=m)
            exec_sql(sql,**config)

            # WITH LINES
            # if os.path.exists("./temp/temp.csv"):
            #     os.remove("./temp/temp.csv")
            # cursor = conn.cursor()
            # for col in df.columns.to_list():
            #     print(f'Adding {col} to the CSV...')
                
            #     df[col].to_csv('./temp/temp.csv', header=False, index=False, lineterminator=',', mode='a')
            #     with open('./temp/temp.csv', 'a') as csv_file :
            #         csv_file.write('\n')

            # print('Copying the CSV to the database')
            # with open('./temp/temp.csv', 'r') as csv_file :
            #     cursor.copy_from(csv_file,table = '{t}_tripdata_{y}_{m:02d}'.format(t=taxi[t_id],y=y,m=m),sep=',',columns=[x.lower() for x in df.columns.to_list()])
            # quit(0)

            #WITH COLUMNS
            print('Writing data to CSV')
            fillna_dict = {'airport_fee':0.0,
                           'passenger_count':0,
                           'store_and_fwd_flag':'O',
                           'ratecodeid':0.0,
                           'congestion_surcharge':0.0}
            # print(df.fillna(value=fillna_dict).loc[[6339568]].RatecodeID)
            # quit(0)
            # df = df.drop([6339568])
            # df.fillna(value=fillna_dict,inplace=True)
            df.fillna(value=fillna_dict,inplace=True)
            df['RatecodeID'] = df['RatecodeID'].fillna(0.0)
            df.to_csv('./temp/temp.csv', header=False, index=False)
            print('Writing CSV to database')
            cursor = conn.cursor()
            with open('./temp/temp.csv', 'r') as csv_file :
                cursor.copy_from(csv_file,table = '{t}_tripdata_{y}_{m:02d}'.format(t=taxi[t_id],y=y,m=m),sep=',',columns=[x.lower() for x in df.columns.to_list()])
            # cursor.commit
            print('Done')
            quit(0)

        ce_id+=1

    # for y in year :
    #     for m in month :
    #         for t in taxi :
    #             df = pd.read_parquet(f'./data/{t}/{y}/{t}_tripdata_{y}-{m}.parquet',engine='fastparquet')
    #             df.to_sql(f'{t}_tripdata_{y}-{m}', engine, chunksize=int(df.shape[0]/100), if_exists='fail')