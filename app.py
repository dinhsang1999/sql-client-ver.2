import pyodbc as po
import sqlalchemy as sc
import pandas as pd
import argparse as arg
import sys
import os
import string
import random
import socket

def main():
    """
    It's trying to connect to the database, and if it fails, it prints the error message. Then it's
    trying to create a cursor object, and if it fails, it prints the error message. Then it's trying to
    create a table, and if it fails, it prints the error message. Then it's trying to insert data into
    the table, and if it fails, it prints the error message
    """
    conx_string = connection_string(config["driver"],config["server"],config["database"])
   # It's trying to connect to the database, and if it fails, it prints the error message.
    try:
        conx = po.connect(conx_string)
    except po.DatabaseError as e:
        print('Database Error:',e)
    except po.Error as e:
        print('Conenction Error:')
        print(str(e.value[1]))

    # It's trying to create a cursor object, and if it fails, it prints the error message.
    try:
        cursor = conx.cursor()
    except Exception as e:
        print(str(e[1]))
    
    data =pd.read_csv(config["path"])
    df = pd.DataFrame(data)
    columns = list(df.columns)
    # It's creating a table name with random letters.
    letters = string.ascii_lowercase
    name = ''.join(random.choice(letters) for i in range(5))
    create_table = "CREATE TABLE " + name + " ("
    for i in range(len(columns)):
        create_table = create_table +columns[i].upper() + " nvarchar(200),"
    create_table = create_table[:-1] + ")"

    cursor.execute(create_table)
    conx.commit()

    # https://www.youtube.com/watch?v=eEVG-A4R9WU
    if "OBDC" in config["driver"]: 
        conn = sc.create_engine(f"mssql+pyobdc://{config['server']}/{config['database']}?trusted_connection=yes&driver={config['driver']}") #Fast but need OBDC driver
        data.to_sql(name,con=conn,if_exists="append",index=False)
    else:
        for row in df.itertuples():
            cursor.execute(f'''
                        INSERT INTO {name} (Year, Variable_name, Value)
                        VALUES (?,?,?)
                        ''',
                        row.Year, 
                        row.Variable_name,
                        row.Value
                        )
        print(f"Completed! Push csv file onto {name}")
        conx.commit()
        cursor.close()

def connection_string(driver_,server_name,database_name):
    """
    It takes three arguments, and returns a string that can be used to connect to a SQL Server database.
    
    :param driver: the driver you're using to connect to the database
    :param server_name: The name of the server you want to connect to
    :param database_name: The name of the database you want to connect to
    :return: A string
    """
    driver_ = "{" + driver_ + "}"
    conn_string = f"""
    DRIVER={driver_};
    SERVER={server_name};
    database={database_name};
    trusted_connection= yes;
    """
    return conn_string

if __name__ == '__main__':
    '''
    Input variables from command line
    Usage:
        options:
                    -h, --help            show this help message and exit
                    -d DRIVER, --driver DRIVER
                                            Run driver.py to check
                    -s SERVER, --server SERVER
                                            Server's name
                    -D DATABASE, --database DATABASE
                                            Database's name
                    -m, --mode            True: push csv on database;False: get table on database and save
                    -p PATH, --path PATH  csv's path
    '''
    parser = arg.ArgumentParser(description="COMUNICATE-SERVER-CLIENT")
    parser.add_argument('-d','--driver',help = "Run driver.py to check",required=True)
    parser.add_argument('-s','--server',help = "Server's name",required=True)
    parser.add_argument('-D','--database',help = "Database's name",required=True)
    parser.add_argument('-m','--mode', action='store_false',help = "True: push csv on database;False: get table on database and save")
    parser.add_argument('-p','--path',help = "csv's path")

    args = parser.parse_args(sys.argv[1:])
    config = vars(args)

    if config["mode"]:
        if config["path"] == None:
            print("app.py: error: the following arguments are required: path \n please, --help to know what path to")
            exit()
        if os.path.exists(config["path"]):
            pass
        else:
            print("csv file does not exist, please check path again")
            exit()
    else:
        pass

    main()

