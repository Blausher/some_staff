import requests
import ast
import pandas as pd
import openpyxl
import re 
import os
from bs4 import BeautifulSoup
import pandas as pd
import time
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# from fake_useragent import UserAgent
# UserAgent().chrome
# from requests.auth import HTTPBasicAuth

from selenium import webdriver
from selenium.webdriver.chrome.service import Service


class Parser:
    def __init__(self, list_of_urls: list):   #добавить pages?
        '''Вход: [url1, url2, ...] \n
           Список необходимых ссылок для КА'''
        self.list_of_urls = list_of_urls
        self.dict_zaprosov = None
        self.dct_of_tables = None


    def yandex_market_parcer(self) -> dict:
        '''Вход: список ссылок
        Выход: словарь {Зарос: [ссылка1, ссылка2, ...]}'''

        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(options=options)

        self.dict_zaprosov = {}
        pages = [1,2]                         
        for url in self.list_of_urls:

            url_link_list = []  #список для одной url
            for page in pages:
                zapros = driver.get(url) #запрос к странице
                input('pres smthn to continue')
                time.sleep(np.random.randint(low=1, high=4)) 
                bs = BeautifulSoup(driver.page_source)

                title = bs.find_all('h1')[0].string #название запроса

                link_list = []
                for i in bs.find_all('a', {'class' : 'egKyN'}):
                    link = 'https://market.yandex.ru/' + i.get('href')
                    if link not in link_list:
                        link_list.append(link)
                
                url_link_list += url_link_list  #добавляем к списку для url

            self.dict_zaprosov[title] = link_list[:-1]   #добавляем список ссылок по ключу названия запроса без последнего элемента,
                                                    # тк там ненужная инфа

        return self.dict_zaprosov


    def dict_of_urls_parcer(self) -> dict:
        '''Вход: словарь с сылками 
        Выход: словарь {Запрос: [таблица1, таблица2, ...]}'''

        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(options=options)

        self.dct_of_tables = {}
        dict_of_urls = self.dict_zaprosov
        for key, url_list in dict_of_urls.items():

            df = pd.DataFrame(columns = ['name', 'unparsed_price', 'price', 'url', 'time'])
            for counter, url in enumerate(url_list): #идем по ссылке
                
                zapros = driver.get(url) #запрос к странице
                input('pres smthn to continue')
                time.sleep(np.random.randint(low=1, high=4))

                bs = BeautifulSoup(driver.page_source)

                s = bs.find('div', {'class' : '_2pwTb zR0dJ'}).span.next_element.next_element #где лежит цена
                pattern = re.compile(r'\d{1,}')
                price = re.findall(pattern, s)

                name = bs.find('h1').string #имя ссылки

                now = datetime.now()
                dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

                df.loc[counter] = [name, s, price, url, dt_string]

                print('progress:', (counter+1)/len(url_list))
            print(f'Данные скачаны для запроса: {key}')

            self.dct_of_tables[key] = df #добавляем в словарь поисковой запрс и таблицу
        
        return self.dct_of_tables


    def combine_tables_together(self):
        #создаем новую папку, если ее нет (downloaded_data)
        current_directory = os.getcwd()
        final_directory = os.path.join(current_directory, r'downloaded_data')
        if not os.path.exists(final_directory):
            os.makedirs(final_directory)

        with pd.ExcelWriter('downloaded_data/full_df.xlsx') as writer: #общий эксель
            for key, table in self.dct_of_tables.items():
                key = key[:29]
                table['price'] = table['price'].apply(lambda x: "".join(str(element) for element in x)).astype(int)
                table = table.sort_values(by = 'price')
                table = table.drop(columns = 'unparsed_price')

                table.to_excel(writer, sheet_name=key, index=False) #сохраняем в общую таблицу
        
        writer = writer.close() #завершаем работу с этим файлом


    def parse_data(self):
        print('start')
        step1 = self.yandex_market_parcer()
        print('stage 1 completed')
        step2 = self.dict_of_urls_parcer()
        print('stage 2 completed')
        step3 = self.combine_tables_together()
        print('stage 3 completed')



    