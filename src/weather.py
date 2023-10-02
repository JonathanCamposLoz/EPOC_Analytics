
import json
from json import JSONDecodeError
import pandas as pd
import os
from dotenv import load_dotenv #pip install python-dotenv

import requests
import datetime
from .db_datos import Db_datos


class Weather():


    def __init__(self):
        load_dotenv()
        
        self.__con = Db_datos()
        self.__url = 'http://api.openweathermap.org/'

        self.__ubication = {'bogota': {'lat': '4.710989','lon': '-74.072092'}}        



    def get_dataWeather(self, ciudad):

        self.endpoint = str('data/2.5/weather?lat={}&lon={}&appid={}').format(
            self.__ubication.get(ciudad).get('lat'),
            self.__ubication.get(ciudad).get('lon'),
            os.getenv('KEY_OPENWEATHER')
        )
        
        self.headers = {
            "Content-Type": "application/json"
        }
        response = requests.get(self.__url + self.endpoint, headers=self.headers)
        if response.status_code == 200:
            try:
                # Obtener el token de la respuesta
                data = response.json()
                self.etl_weather(data)
                print('Process ok')
            except json.decoder.JSONDecodeError as e:
                print("Error de decodificación JSON:", str(e))
                return ''  
            

    def etl_weather(self,datos):
        #https://openweathermap.org/current
        try:
            df_weather = pd.DataFrame(datos.get('weather') , index=[0]) 
            df_main = pd.DataFrame(datos.get('main'), index=[0])
            df_wind = pd.DataFrame(datos.get('wind'), index=[0])
            df_sys = pd.DataFrame(datos.get('sys'), index=[0])
            df_sys = df_sys.set_axis(['type','id_sys','country','sunrise','sunset'], axis=1)
            df_all = df_weather.join([ 
                                df_main, 
                                df_wind, 
                                df_sys], how='inner')
            df_all['base'] = datos.get('base')
            df_all['visibility'] = datos.get('visibility')
            fecha_normal = datetime.datetime.fromtimestamp(datos.get('dt'))
            df_all['date'] = fecha_normal.strftime('%Y-%m-%d')
            
            df_all['timezone'] = datos.get('timezone')
            df_all['name'] = datos.get('name')

            dc_toPush = [{'column_sql': 'id', 'column_df': 'id'},
                        {'column_sql': 'main', 'column_df': 'main'},
                        {'column_sql': 'description', 'column_df': 'description'},
                        {'column_sql': 'icon', 'column_df': 'icon'},
                        {'column_sql': 'temp', 'column_df': 'temp'},
                        {'column_sql': 'feels_like', 'column_df': 'feels_like'},
                        {'column_sql': 'temp_min', 'column_df': 'temp_min'},
                        {'column_sql': 'temp_max', 'column_df': 'temp_max'},
                        {'column_sql': 'pressure', 'column_df': 'pressure'},
                        {'column_sql': 'humidity', 'column_df': 'humidity'},
                        {'column_sql': 'speed', 'column_df': 'speed'},
                        {'column_sql': 'deg', 'column_df': 'deg'},
                        {'column_sql': 'type', 'column_df': 'type'},
                        {'column_sql': 'id_sys', 'column_df': 'id_sys'},
                        {'column_sql': 'country', 'column_df': 'country'},
                        {'column_sql': 'sunrise', 'column_df': 'sunrise'},
                        {'column_sql': 'sunset', 'column_df': 'sunset'},
                        {'column_sql': 'base', 'column_df': 'base'},
                        {'column_sql': 'visibility', 'column_df': 'visibility'},
                        {'column_sql': 'date', 'column_df': 'date'},
                        {'column_sql': 'timezone', 'column_df': 'timezone'},
                        {'column_sql': 'name', 'column_df': 'name'}]
            
            self.__con.pushData(df_all,'weather',False,dc_toPush)
        except:
            pass


        
