from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy import String
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session

import psycopg2
import pandas as pd
import itertools
import os
import datetime
from io import StringIO

from src.load import exec_sql

if __name__=='__main__' :

    download = False

    year = [y for y in range(2009,2026)]
    month = [m for m in range(1,13)]
    taxi = {1:'yellow', 2:'green', 3:'for_hire_vehicle', 4:'high_volume_for_hire_vehicle'}
    config = {
        'host' : 'localhost',
        'port' : 5432,
        'user' : 'admin',
        'password' : 'root',
        'database' : 'nyc_trip_record'}

    engine = create_engine(f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")
 
    class Base(DeclarativeBase):
        pass

    class Taxi(Base):
        __tablename__ = "taxi_list"

        type_id:                Mapped[int] = mapped_column(primary_key=True)
        taxi_type:              Mapped[str]

    class Calendar(Base):
        __tablename__ = "calendar"

        calendar_entry_id:      Mapped[int] = mapped_column(primary_key=True)
        taxi_type_id:           Mapped[int]
        year:                   Mapped[int]
        month:                  Mapped[int]

    class Yellow(Base):
        __tablename__ = "yellow_taxi"

        TripID:                 Mapped[int] = mapped_column(primary_key=True)
        VendorID:               Mapped[int]
        tpep_pickup_datetime:   Mapped[datetime.datetime]
        tpep_dropoff_datetime:  Mapped[datetime.datetime]
        passenger_count:        Mapped[float]
        trip_distance:          Mapped[float]                
        RatecodeID:             Mapped[float]
        store_and_fwd_flag:     Mapped[str]
        PULocationID:           Mapped[int]
        DOLocationID:           Mapped[int]
        payment_type:           Mapped[int]
        fare_amount:            Mapped[float]
        extra:                  Mapped[float]
        mta_tax:                Mapped[float]
        tip_amount:             Mapped[float]
        tolls_amount:           Mapped[float]
        improvement_surcharge:  Mapped[float]
        total_amount:           Mapped[float]
        congestion_surcharge:   Mapped[float]
        airport_fee:            Mapped[float]
        calendar_entry_id:      Mapped[int]

    yellow_tripdata = {}
    

    # Reset everyhing
    Taxi.__table__.drop(engine, checkfirst=True)
    Calendar.__table__.drop(engine, checkfirst=True)
    Yellow.__table__.drop(engine, checkfirst=True)

    Base.metadata.create_all(engine)

    try :
        conn = psycopg2.connect(**config)
        conn.autocommit = True
    except psycopg2.Error as e :
        print('Error connecting to the database')
        print(e)
    else :
        print('Successfully connected to database.')

    with Session(engine) as session:

        # Fill the taxi table
        for t in taxi:
            t = Taxi(
                type_id = t,
                taxi_type = taxi[t]
            )
            session.add(t)
            session.flush()

        ce_id = 1
        for t,y,m in itertools.product(taxi,year,month): 

            calendar_entry = Calendar(
                calendar_entry_id = ce_id,
                taxi_type_id = t,
                year = y,
                month = m
            )
            session.add(calendar_entry)
            session.flush()

            tablename = "{t}_tripdata_{y}_{m:02d}".format(t=taxi[t],y=y,m=m)
            Yellow.__tablename__ = tablename
            
            pq_file = './data/{t}/{y}/{t}_tripdata_{y}-{m:02d}.parquet'.format(t=taxi[t],y=y,m=m)

            if os.path.isfile(pq_file):
                print(f"Loading {pq_file} in Pandas...")
                df = pd.read_parquet(pq_file,engine='fastparquet')
            else:
                ce_id+=1
                continue

            print('Writing data to CSV')
            fillna_dict = {'airport_fee':0.0,
                           'passenger_count':0,
                           'store_and_fwd_flag':'O',
                           'ratecodeid':0.0,
                           'congestion_surcharge':0.0}

            df.fillna(value=fillna_dict,inplace=True)
            df['RatecodeID'] = df['RatecodeID'].fillna(0.0)

            #buffer = StringIO()
            df.to_csv("./temp/temp.csv", header=False, index=False)
            #buffer.seek(0)

            print("Pushing to database")
            cursor = conn.cursor()
            with open('./temp/temp.csv', 'r') as csv_file :
                cursor.copy_from(csv_file,table = '{t}_tripdata_{y}_{m:02d}'.format(t=taxi[t],y=y,m=m),sep=',',columns=[x.lower() for x in df.columns.to_list()])

            print('Done')

            #session.commit()
            #quit(0)

            ce_id += 1

        session.commit()
    # print(yellow_tripdata)
        
    quit(0)

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

        if os.path.isfile(pq_file) :
            print(f'Loading {pq_file} into pandas...')
            df = pd.read_parquet(pq_file,engine='fastparquet')
            df = df.assign(calendar_entry_id = ce_id) 
            
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


            #WITH COLUMNS
            print('Writing data to CSV')
            fillna_dict = {'airport_fee':0.0,
                           'passenger_count':0,
                           'store_and_fwd_flag':'O',
                           'ratecodeid':0.0,
                           'congestion_surcharge':0.0}

            df.fillna(value=fillna_dict,inplace=True)
            df['RatecodeID'] = df['RatecodeID'].fillna(0.0)
            df.to_csv('./temp/temp.csv', header=False, index=False)
            print('Writing CSV to database')
            cursor = conn.cursor()
            with open('./temp/temp.csv', 'r') as csv_file :
                cursor.copy_from(csv_file,table = '{t}_tripdata_{y}_{m:02d}'.format(t=taxi[t_id],y=y,m=m),sep=',',columns=[x.lower() for x in df.columns.to_list()])

            print('Done')
            quit(0)

        ce_id+=1
