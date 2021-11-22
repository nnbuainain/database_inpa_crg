import psycopg2, time
from utils.utils import config

def create_connection():
    conn = None

    try:
        # read connection parameters
        params = config()

        print('\nConnecting to the PostgreSQL database...')
        time.sleep(1)
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        print('PostgreSQL database version:')
        cur.execute('SELECT version()')
        db_version = cur.fetchone()
        print(db_version)

        cur.close()
        time.sleep(1)

        return conn

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def select_register(conn, select_query: str) -> int:
    cur = conn.cursor()

    try:
        cur.execute(select_query)
        return cur.rowcount

    except (Exception, psycopg2.DatabaseError) as error:
        print(f'\nSelect error ! {error}')

def delete_register(conn, delete_query: str) -> int:
    cur = conn.cursor()

    try:
        cur.execute(delete_query)
        return cur.rowcount

    except (Exception, psycopg2.DatabaseError) as error:
        print(f'\nData could not be deleted! {error}')
        return 0
    finally:
        conn.commit()

def insert_register(conn, insert_query: str, data: list) -> int:
    cur = conn.cursor()

    try:
        records_list_template = ','.join(['%s'] * len(data))
        insert_query = insert_query + ' {}'.format(records_list_template)

        cur.execute(insert_query, data)
        return cur.rowcount

    except (Exception, psycopg2.DatabaseError) as error:
        print(f'\nData could not be inserted! {error}')
        return 0
    finally:
        conn.commit()

