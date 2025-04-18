import psycopg2

def exec_sql (sql,host,port,user,password,database) :
    try:
        with  psycopg2.connect(host = host,port = port,user = user,password = password,database = database) as conn:
            with  conn.cursor() as cur:
                # execute the INSERT statement
                cur.execute(sql)
                # commit the changes to the database
                conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print('An error occured :')
        print(error)