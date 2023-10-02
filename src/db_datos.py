#import MySQLdb
import pymysql
import datetime
import os
from dotenv import load_dotenv
from datetime import date
from sqlalchemy import create_engine
import pandas as pd


"""
This class provides methods to connect to a MySQL database, run queries, and load data.
The `__init__()` method initializes the class and gets the database connection information from the `.env` file.
The `run_query()` method runs a query on the database and returns the results.
The `extracData()` method extracts data from the database and returns a Pandas DataFrame.
The `pushData()` method loads data into the database.
"""

class Db_datos():

    def __init__(self):
        load_dotenv()
        self.DB_HOST = os.getenv('DB_HOST_EPOC')
        self.DB_USER = os.getenv('DB_USER_EPOC')
        self.DB_PASS = os.getenv('DB_PASS_EPOC')
        self.DB_NAME = os.getenv('DB_NAME_EPOC')

    def run_query(self, query=''): 
        con = pymysql.connect(host=self.DB_HOST,
                user=self.DB_USER,
                password=self.DB_PASS,
                db=self.DB_NAME,
                cursorclass=pymysql.cursors.DictCursor)
        try:
            with con.cursor() as cur:
                if query.upper().startswith('SELECT') or query.upper().startswith('WITH'): 
                    cur.execute(query)
                    data = cur.fetchall()   # Traer los resultados de un select 
                else: 
                    cur.execute(query)       # Hacer efectiva la escritura de datos 
                    con.commit()
                    data = None     
            cur.close()                 # Cerrar el cursor 
            con.close()                   # Cerrar la conexión 
            return data
        except Exception as e:
            print(str(e))

    def extracData(self, sql):
        addres = str('mysql+pymysql://{}:{}@{}/{}').format(self.DB_USER, self.DB_PASS,self.DB_HOST,self.DB_NAME)
        db_connection_str = addres
        db_connection = create_engine(db_connection_str)

        df_respuesta = pd.read_sql(sql, con=db_connection)
        return df_respuesta

    def pushData(self, df_data, table, clear_table, dictionary):
        try:
            if clear_table == True:
                sql_clear = str('delete from {}').format(table)
                self.run_query(sql_clear)
            
            columns_sql = str([f"{col['column_sql']}" for col in dictionary]).replace("[","").replace("]","").replace("'","")

            renglon= ''
            contador = 0
            num_cargas = 0
            count_total = 0
            for i in range(len(df_data)):
                linea = []
                count_total += 1
                for j in dictionary:
                    linea.append(str(df_data[j['column_df']][i]))
                renglon += f"({str(linea).replace('[','').replace(']','')}),"
                contador += 1
                if contador == 10000:
                    num_cargas += 1
                    sql = str('insert into {} ({}) value {};').format(table,columns_sql,str(renglon[:-1]))
                    self.run_query(sql)
                    #print(f'Num Carga {num_cargas}')
                    renglon = ''
                    contador = 0
            sql = str('insert into {} ({}) value {};').format(table,columns_sql,str(renglon[:-1]))
            self.run_query(sql)
            
            return count_total
        except Exception as e:
            print('Error => ', e)
            return count_total
            
