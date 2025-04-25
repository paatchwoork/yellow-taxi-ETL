import os
import pandas as pd

def load_data (taxi, year, month, conn):
    tablename = "{t}_tripdata_{y}_{m:02d}".format(t=taxi,y=year,m=month)
    pq_file = './data/{t}/{y}/{t}_tripdata_{y}-{m:02d}.parquet'.format(t=taxi,y=year,m=month)
            
    print(f"Loading {pq_file} in Pandas...")
    df = pd.read_parquet(pq_file,engine='fastparquet')

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
