# The connection is used to establish the connection to the Postgres instance,
from airflow.providers.postgres.hooks.postgres import PostgresHook

# To interact with our Postgres database using Python.
# We will use a very common adapter called Psycopg2.
# And the first import we will need is a package from the Psycopg2 that is called the real data set.
# This will allow retrieval of reports using Python dictionaries and not the default tuples.
from psycopg2.extras import RealDictCursor

table="yt_api"
# define helper function to start postgres server and database establsh 
def get_conn_cursor():
    # postgres_conn_id -> from docker compose ymal file 
    # databse -> from .env
    hook= PostgresHook(postgres_conn_id="postgres_db_yt_elt",database="elt_db")
    conn= hook.get_conn()
    cur=conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

#define helper function to close connection
def close_conn_cursor(conn,cur):
    cur.close()
    conn.close()

# define helper to create databse schemas
def create_schema(schema):
    conn, cur=get_conn_cursor()
    schema_sql=f"CREATE SCHEMA IF NOT EXISTS {schema}"
    cur.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cur)

# define helper to create databse schema tables
def create_table(schema):
    conn, cur=get_conn_cursor()
    if schema=="staging":
        table_sql=f"""
                CREATE TABLE IF NOT EXISTS  {schema}.{table}(
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
                )
            """
    else:
         #expected core schema
         table_sql = f"""
                  CREATE TABLE IF NOT EXISTS {schema}.{table} (
                      "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                      "Video_Title" TEXT NOT NULL,
                      "Upload_Date" TIMESTAMP NOT NULL,
                      "Duration" TIME NOT NULL,
                      "Video_Type" VARCHAR(10) NOT NULL,
                      "Video_Views" INT,
                      "Likes_Count" INT,
                      "Comments_Count" INT    
                  ); 
              """
   
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)


# define helper to get video_ids
def get_video_ids(cur,schema):
    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids=cur.fetchall()
    video_ids=[row["Video_ID"] for row in ids]

    return video_ids