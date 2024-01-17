from decouple import config
import sys
import argparse
import pandas as pd
from sqlalchemy import create_engine

def fetch_data(dbstring,sql):
    engine = None
    df = None
    try:
        engine = create_engine(dbstring).connect()
        df = pd.read_sql_query(sql,con=engine)
    except Exception as error:
        print(error)
        sys.exit(1)
    finally:
        if engine is not None:
            engine.close()
        return df

def load_dataframe_to_postgres(df,table_name,schema,dbstring):
    engine = None
    try:
        engine = create_engine(dbstring,connect_args={'options': f'-csearch_path={schema}'})
        with engine.connect() as conn:
            if not engine.dialect.has_table(conn, table_name):
                df.head(0).to_sql(table_name, conn, if_exists='replace', index=False)
            df.to_sql(table_name,conn, if_exists='replace', index=False)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if engine is not None:
            engine.dispose()
    
def main():
    #print("LEYENDO CSV")
    raw_df = pd.read_csv(file_path)
    raw_df.info()
    load_dataframe_to_postgres(raw_df,table_name,schema,dbstring)
    #raw_df.head(5)
    
    
if __name__=='__main__':
    parser = argparse.ArgumentParser(description="CSV File ingestion tool")
    parser.add_argument('-fp', '--file_path', help="full path to csv")
    parser.add_argument('-t', '--table_name', help="table name")    
    args = parser.parse_args(args=sys.argv[1:])

    engine = None
    username = config('user')
    hostname = config('hostname')
    database = config('database')
    pwd = config('pwd')
    schema = config('schema')
    port_id = config('port_id')
    
    if args.table_name is not None:
        table_name = args.table_name
    else:
        print("PLEASE DONT FORGET TO INCLUDE TABLE_NAME")
        exit(1)
    # databasetype:// username:password@hostname:port/database
    dbstring="postgresql://{0}:{1}@{2}:{3}/{4}".format(username,pwd,hostname,port_id,database)
    file_path = args.file_path
    main()