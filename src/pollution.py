import json
from json import JSONDecodeError
import pandas as pd
import os
from dotenv import load_dotenv #pip install python-dotenv

import requests
import datetime
from .db_datos import Db_datos


class Pollution():

    def __init__(self):
        load_dotenv()

        self.__con = Db_datos()
        self.__url = 'http://api.openweathermap.org/'
        # Resolución 2254 de 2017
        self.__classification = {'1': {'range': '0 to 50','color': 'Green','Level': 'Good'},
                                 '2': {'range': '51 to 100','color': 'Yellow','Level': 'Moderate'},
                                 '3': {'range': '101 to 150','color': 'Orange','Level': 'Unhealthy for sensitive groups'},
                                 '4': {'range': '151 - 200','color': 'Red','Level': 'Unhealthy'},
                                 '5': {'range': '201-300','color': 'Purple','Level': 'Very unhealthy'},
                                 '6': {'range': '301-500','color': 'Maroon','Level': 'Hazardous'},
                                 '7': {'range': '501-1000','color': 'Brown','Level': 'Very Hazardous'}}
        
        self.__ubication = {'bogota': {'lat': '4.710989','lon': '-74.072092'}}        

    def get_dataPollution(self, ciudad):
        self.endpoint = str('data/2.5/air_pollution?lat={}&lon={}&appid={}').format(
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
                self.etl_pushPollution(data)
                print('Process ok')
            except json.decoder.JSONDecodeError as e:
                print("Error de decodificación JSON:", str(e))
                return ''        

    def etl_pushPollution(self,datos):
        # https://openweathermap.org/api/air-pollution
        try:
            components = datos.get('list')[0].get('components')

            df_data = pd.DataFrame(components, index=[0])
            df_data['lon'] = datos.get('coord').get('lon')
            df_data['lat'] = datos.get('coord').get('lat')
            
            aqi= datos.get('list')[0].get('main').get('aqi')
            df_data['aqi'] = aqi
            df_data['range'] =self.__classification.get(str(aqi)).get('range')
            df_data['color'] =self.__classification.get(str(aqi)).get('color')
            df_data['Level'] =self.__classification.get(str(aqi)).get('Level')

            fecha_normal = datetime.datetime.fromtimestamp(datos.get('list')[0].get('dt'))
            df_data['date'] = fecha_normal.strftime('%Y-%m-%d')

            dc_toPush = [{'column_sql': 'co', 'column_df': 'co'},
                        {'column_sql': 'no', 'column_df': 'no'},
                        {'column_sql': 'no2', 'column_df': 'no2'},
                        {'column_sql': 'o3', 'column_df': 'o3'},
                        {'column_sql': 'so2', 'column_df': 'so2'},
                        {'column_sql': 'pm2_5', 'column_df': 'pm2_5'},
                        {'column_sql': 'pm10', 'column_df': 'pm10'},
                        {'column_sql': 'nh3', 'column_df': 'nh3'},
                        {'column_sql': 'lon', 'column_df': 'lon'},
                        {'column_sql': 'lat', 'column_df': 'lat'},
                        {'column_sql': 'aqi', 'column_df': 'aqi'},
                        {'column_sql': 'range_h', 'column_df': 'range'},
                        {'column_sql': 'color', 'column_df': 'color'},
                        {'column_sql': 'Level', 'column_df': 'Level'},
                        {'column_sql': 'date', 'column_df': 'date'}]
            self.__con.pushData(df_data,'pollution',False,dc_toPush)
        except:
            pass
