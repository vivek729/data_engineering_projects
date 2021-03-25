import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    
    Parameters:
        cur: the cursor object.
        conn: database connection object.  
    """
    print("Dropping existing tables...")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print("Dropped.")


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    
    Parameters:
        cur: the cursor object.
        conn: database connection object. 
    """
    print("Creating tables...")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("Created.")


def main():
    """ 
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("connecting...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("connected")
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()