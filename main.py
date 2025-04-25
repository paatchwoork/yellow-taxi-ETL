from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy import String
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, String, Integer, Float, DateTime, ForeignKey

import psycopg2
import pandas as pd
import itertools
import os
import datetime
from io import StringIO

from src.load import load_data

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

    # Create the engine
    engine = create_engine(f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}")#, echo=True)
 
    metadata = MetaData()

    # Create the Base and the Taxi and Calendar Tables usinf the declarative base method
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
    
    # Reset everyhing
    Taxi.__table__.drop(engine, checkfirst=True)
    Calendar.__table__.drop(engine, checkfirst=True)

    Base.metadata.create_all(engine)

    # Connect to the database
    try :
        conn = psycopg2.connect(**config)
        conn.autocommit = True
    except psycopg2.Error as e :
        print('Error connecting to the database')
        print(e)
    else :
        print('Successfully connected to database.')

    if download :
        pass

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

            # yellow.drop(bind = engine, checkfirst=True)
            # yellow.metadata.create_all(engine)
            pq_file = './data/{t}/{y}/{t}_tripdata_{y}-{m:02d}.parquet'.format(t=taxi[t],y=y,m=m)

            # print(os. getcwd())
            if os.path.isfile(pq_file):
                tablename = "{t}_tripdata_{y}_{m:02d}".format(t=taxi[t],y=y,m=m)
                
                yellow = Table(
                    tablename,
                    metadata,
                    Column("TripID", Integer, primary_key=True),                 
                    Column("VendorID", Integer),
                    Column("tpep_pickup_datetime", DateTime),
                    Column("tpep_dropoff_datetime", DateTime),
                    Column("passenger_count", Float),
                    Column("trip_distance", Float),               
                    Column("RatecodeID", Float),
                    Column("store_and_fwd_flag", String),
                    Column("PULocationID", Integer),
                    Column("DOLocationID", Integer),
                    Column("payment_type", Integer),
                    Column("fare_amount", Float),
                    Column("extra", Float),
                    Column("mta_tax", Float),
                    Column("tip_amount", Float),
                    Column("tolls_amount", Float),
                    Column("improvement_surcharge", Float),
                    Column("total_amount", Float),
                    Column("congestion_surcharge", Float),
                    Column("airport_fee", Float),
                    Column("calendar_entry_id", Integer, ForeignKey(Calendar.calendar_entry_id, ondelete = 'CASCADE'))
                )
                metadata.create_all(bind = engine, tables = [yellow])

                load_data (taxi[t],y,m, conn)
                
            ce_id += 1

        session.commit()