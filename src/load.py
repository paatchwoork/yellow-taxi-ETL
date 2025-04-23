import psycopg2
import os
import pandas as pd

def load_data (tablename, conn):
    pq_file = f'./data/{tablename.replace('-','_')}.parquet'

    if os.path.isfile(pq_file):
        print(f"Loading {pq_file} in Pandas...")
        df = pd.read_parquet(pq_file,engine='fastparquet')
    else:
        print(f"File {pq_file} doesn't exist.")
        return None
        # ce_id+=1
        # continue

    print('Writing data to CSV')
    fillna_dict = {'airport_fee':0.0,
                    'passenger_count':0,
                    'store_and_fwd_flag':'O',
                    'ratecodeid':0.0,
                    'congestion_surcharge':0.0}

    df.fillna(value=fillna_dict,inplace=True)
    df['RatecodeID'] = df['RatecodeID'].fillna(0.0)

    df.to_csv("./temp/temp.csv", header=False, index=False)

    print("Pushing to database")
    cursor = conn.cursor()
    with open('./temp/temp.csv', 'r') as csv_file :
        cursor.copy_from(csv_file,table = tablename, sep=',', columns = df.columns.to_list())

    print('Done')
