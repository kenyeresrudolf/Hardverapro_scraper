# -*- coding: utf-8 -*-
"""
Created on Sun Nov 13 16:19:53 2022
@author: RUID


Copyright [yyyy] [name of copyright owner]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
 
from datetime import date
from datetime import timedelta

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
 
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import datetime as dt

 #%%


scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive.file',
         'https://www.googleapis.com/auth/drive']

# Reading Credentails from ServiceAccount Keys file
credentials = ServiceAccountCredentials.from_json_keyfile_name(r'F://SCRPAER//GSheet_API/credentials_0129_v2.json'
            , scope)
# intitialize the authorization object            
gc = gspread.authorize(credentials)

#%%
sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1jXP01P6VwOqPc8VIijAhzxowCuiVtC9hXa62fY6MlEc/edit#gid=1343084875')  
worksheet1 = sheet.worksheet('decupdate') 
 
#%% creating yesterday's df

old_df = pd.DataFrame(worksheet1.get_all_records())

 
  #%% 

def get_dropdown_item_elements(soup):
    '''Identifying the number of pages as it is necessary for the iteration process'''
    ha_pagination = soup.find('div',
                              class_='navbar navbar-default pager-navbar justify-content-center justify-content-md-between')
    ha_pagination_all = ha_pagination.find('a', class_='dropdown-item')

    items = []
    for each_div in ha_pagination_all:
        print(each_div)
        items.append(each_div)

    return items


def get_links(last_page):
    '''collecting hyperlinks'''
    links = []

    for x in range(0, last_page, 50):
        r = requests.get(f'https://hardverapro.hu/aprok/hardver/videokartya/index.html?offset={x}')
        soup = BeautifulSoup(r.content, 'lxml')
        products = soup.find_all('div', class_='uad-title')
        for link in products:
            y = link.find('a', href=True)
            links.append(y['href'])
            print(y)

    return links

def badcat_filter_price (keyword):
    '''filtering false namings from prie column'''
    newdf = df.loc[(df.price==keyword)]
    return newdf


def stringmatcher (column_name_from_df,benchmark_table_column,benchmark_table_name):
    '''finding the similar based on string'''
    namecheck_base=df[column_name_from_df].astype(str)
    matches=[]
    for i in namecheck_base: 
        most_similar = process.extractOne(i, benchmark_table_name[benchmark_table_column], scorer=fuzz.token_set_ratio, score_cutoff=90)
        try:
            matches.append(most_similar[0])
            print(i, '____', most_similar[0])
        except: 
            matches.append(0)
            print(i, '____', 0)
    
    return matches


def stringmatchers_numsonly (column_name_from_df,benchmark_table_column,benchmark_table_name):
    '''finding the similar based on numbers only'''
    namecheck_base=df[column_name_from_df].astype(str).reset_index(drop=True)
    earlier_matches = df['matches_newbm'].reset_index(drop=True)
    matches = []
    for i in range(len(namecheck_base)):
        b=earlier_matches[i]
        if b !=0:
            print('____')
            matches.append(b)
            continue
        most_similar = process.extractOne(namecheck_base[i], benchmark_table_name[benchmark_table_column], scorer=fuzz.WRatio, score_cutoff=90)
        try:
            matches.append(most_similar[0])
            print(i, '____', most_similar[0])
        except: 
            matches.append(0)
            print(i, '____',0)
    return matches

def binary_checker(list_to_check):
    '''searhing for specific strings in strings'''
    name = np.array(df['name'].str.lower())
    check_base =list(map(lambda x: x.lower(), list_to_check))
    checker = []

    for i in range(len(name)):
        n = name[i].lower()
        match = False
        for ext in check_base:
            print(ext)
            if ext in n:
                print('megvan', n, ext)
                checker.append(1)
                match = True
                break
        print('nincs meg', n)
        if match == False:
            checker.append(0)
        print('__________')
    
    return checker

def df_uploader(to_upload_df, worksheet_name):
    '''upload a dataframe to google sheets'''
    sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1jXP01P6VwOqPc8VIijAhzxowCuiVtC9hXa62fY6MlEc/edit#gid=1343084875')  
    worksheet = sheet.worksheet(f'{worksheet_name}')
    worksheet.clear()
    set_with_dataframe(worksheet=worksheet, dataframe=to_upload_df, include_index=False,
    include_column_header=True, resize=True)
    print('Upload done')


def df_uploader_append(to_append_df,worksheet_name):
    '''append a dataframe to google sheets'''
    worksheet = sheet.worksheet(f'{worksheet_name}') 
    old_df = pd.DataFrame(worksheet.get_all_records())
    temp=old_df.append(to_append_df)
    worksheet.clear()
    set_with_dataframe(worksheet=worksheet, dataframe=temp, include_index=False, include_column_header=True, resize=True)
    print('Append done')
    
    


def get_all_products_from_all_pages(last_page):
    '''Collecting the productlinks from all the pages, adding them to a list'''
    product_list=[]
    for x in range(0,last_page,50):
        r = requests.get(f'https://hardverapro.hu/aprok/hardver/videokartya/index.html?offset={x}', headers = headers)
        soup = BeautifulSoup(r.content, 'lxml')
        products = soup.find_all('div', class_='media-body')
        for element in products:
                try: 
                    name = element.find(class_='uad-title').text.strip()
                except: 
                    name=0    
                try:
                    price = element.find(class_='uad-price').text.strip()
                except: 
                    price = 0
                try:
                    location = element.find(class_='uad-light').text.strip()
                except: 
                    location = 0
                try:
                    dealer = element.find(class_='uad-misc')
                    dealer = dealer.find('a').get_text()
                except: 
                    dealer = 0
                try: 
                    date = element.find(class_='uad-ultralight').text.strip()
                except: 
                    date = 0
                try:
                    link = element.find(class_='uad-title')
                    link = link.find('a', href=True)
                    link = link['href']
                except: 
                    link = 0
                
                product = {
                        'name': name,
                        'price': price,
                        'location':location,
                        'dealer': dealer,
                        'date':date,
                        'link':link
                        }
                product_list.append(product)
        time.sleep(3) 

    return product_list

 #%%
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}
r = requests.get(
    'https://hardverapro.hu/aprok/hardver/videokartya/keres.php?search_exac=0&search_title=0&shipping=0&noiced=0')
soup = BeautifulSoup(r.content, 'lxml')

dropdown_pages = get_dropdown_item_elements(soup)
last_dd_item = dropdown_pages[len(dropdown_pages) - 1]
last_page = int(last_dd_item[-5:])

product_list = get_all_products_from_all_pages(last_page)


 #%% 
df = pd.DataFrame(product_list)
 #%% 
now = (dt.datetime.now())
date_time = now.strftime("%m_%d_%Y")
old_df.to_excel(f'old_df_{date_time}.xlsx', engine='xlsxwriter')

 #%% reoving nulls
df = df[df.name != 0]

 #%% converting date cells

today = date.today()
yesterday = today - timedelta(days = 1)
#☺df.loc[df['date'].str.contains('ma'), 'date'] = today
#df.loc[df['date'].str.contains('tegnap'), 'date'] = yesterday
z= ['ma','Előresorolt hirdetés']
df.date = df.date.apply(lambda x: today if 'ma' in x else yesterday if 'tegnap' in x else today if 'hirdetés' in x else x)


  #%% frozencheck, remove the "uploaded  since" part from the title

#df['date']=pd.to_datetime(df['date'])
names = df['name']
frozen_or_promoted = []
name2 = []
for i in names: 
    if 'jegelve' in i.lower():
        frozen_or_promoted.append('frozen')
        i=i[:-17]+' Jegelve'
        name2.append(i)
    elif 'előresorolt' in i.lower():
        frozen_or_promoted.append('paid')
        i=i[:-21]+' Előresorolt'
        name2.append(i)
    else:
        frozen_or_promoted.append(None)
        i=i[:-9]
        name2.append(i)
df['frozen_or_promoted'] = frozen_or_promoted
df['name']=name2
  #%%    date to correct format
date2=df['date'].astype(str).str[0:10]
df['date']=pd.to_datetime(date2, format="%Y/%m/%d")

#%% benchmark table call
benchmarks= pd.read_csv(r'F://SCRPAER//Benchmarks//GPU_UserBenchmarks.csv', header=0) 
benchmarks_subcat= pd.read_excel(r'F://SCRPAER//Benchmarks//Subcats_manual.xlsx', header=0) 
benchmarks['name']=benchmarks['Model'].astype(str)
benchmarks['benchmars_numonly']=benchmarks['name'].str.replace(r'[^0-9]+', '')
 #%% renaming
benchmarks=benchmarks.rename(columns={"Benchmark": "benchy"}, errors="raise")

 #%%  creating adictionary for benchmark names and values
 
dicttionary_bm=dict(zip(benchmarks.benchmars_numonly, benchmarks.benchy))
dicttionary_bm_fullnames=dict(zip(benchmarks.name, benchmarks.benchy))
dicttionary_bm_names=dict(zip(benchmarks.benchmars_numonly, benchmarks.name))
dicttionary_bm_subcats=dict(zip(benchmarks_subcat.Model, benchmarks_subcat.Type))
 
 #%% smaller benchmark table (avoiding cases where the nr of numbers is less than 3)
mask = (benchmarks['benchmars_numonly'].str.len() > 3)
benchmarks_smaller=benchmarks.loc[mask]


 #%%  filterig numbers from strings 
df['namecheck_base_numonly']=df['name'].str.replace(r'[^0-9]+', '').str[:6]

 #%% stringmatching

df['matches_newbm'] = stringmatcher(column_name_from_df = 'name', benchmark_table_column = 'name', benchmark_table_name = benchmarks )
print(df['matches_newbm'].value_counts()[0])

 #%% stringmatching - numbers only 

x= stringmatchers_numsonly(column_name_from_df = 'namecheck_base_numonly', benchmark_table_column = 'benchmars_numonly', benchmark_table_name = benchmarks_smaller )
df['matches_newbm_numnumonly']=x
df['matches_newbm_numnumonly']=df['matches_newbm_numnumonly'].map(dicttionary_bm_names)

 #%% - ordering finalmatches
import numpy as np 
name = np.array(df['name'].str.lower())
matcher = np.array(df['matches_newbm'])
matcher2 = np.array(df['matches_newbm_numnumonly'])
finalmatch = []      
                
counter = 0
for i in range(len(name)):
    m = matcher[i]
    m2= matcher2[i]
    if m !=0:
        finalmatch.append(m)
        print(m)
    else: 
        finalmatch.append(m2)
        print(m2)

df['finalmatch2']=finalmatch
print(df['finalmatch2'].value_counts()[0])

 #%% autosave, collecting the new GPUs
detected_gpus = pd.unique(df[['finalmatch2']].values.ravel())
groupy =df.groupby("finalmatch2", sort=True)["name"].count()
df_save=df

 #%% checking and grouping the cards based on related strings
new=['új', 'uj', 'ujszeru', 'bontatlan']
df['newcheck']=binary_checker(new)

warranty = ['garis', 'garanciás', 'garival', 'garanciával', 'garancia', 'granciális']
df['garicheck']=binary_checker(warranty)
 
damaged= ['hibás', 'hiba', 'hibával', 'sérült', 'alkatrésznek', 'keresek']
df['damagecheck']=binary_checker(damaged)
 
multi = ['videokártyák', 'bazár', 'kártyák']
df['multicheck']=binary_checker(multi)

 #%% - damaged/multi remove
damagedf=df[df.damagecheck==1]
index_names_damagedf = df[ df['damagecheck'] == 1 ].index
df.drop(index_names_damagedf, inplace = True)

multidfdf=df[df.multicheck==1]
index_names_multidf = df[ df['multicheck'] == 1 ].index
df.drop(index_names_multidf, inplace = True)

 #%% brandsearch
df = df.reset_index(drop=True)
import numpy as np 
brandsToCheck = list(map(lambda x: x.lower(), ['amd','xfx','powercolor', 'dell', 'hp', 'inno3d', 'Gigabyte', 'MSI', 'ASUS', 'Zotac', 'evga','msi', 'sapphire', 'nvidia', 'aorus', 'sapphire',  'asrock','gainward','amd','palit']))
brandfound =[]
name = df['name'].str.lower()
counter = 0
for i in range(len(name)):
    n = name[i]
    match = False
    print(i)
    counter +=1
    for ext in brandsToCheck:
        print(ext)
        if ext in n:
            print('megvan', n, ext)
            brandfound.append(ext)
            match = True
            break
    print('nincs meg', n)
    if match == False:
        brandfound.append(0)
    print('__________')
print(counter, len(brandfound))

df['brandfound']=brandfound
df['brandfound']=df['brandfound'].str.lower()

 #%% retrurning benchmarks, subcategories based on the keys 
dicttionary_bm_namespoints=dict(zip(benchmarks.name, benchmarks.benchy))
df['benchmark2'] = df['finalmatch2'].map(dicttionary_bm_namespoints)
df['subcat'] = df['finalmatch2'].map(dicttionary_bm_subcats)

 #%% creating df for free, bartel and GPU searchers 
ingyendf = badcat_filter_price(keyword = 'Ingyen')
cseredf = badcat_filter_price(keyword = 'Csere')
keresemdf = badcat_filter_price(keyword = 'Keresem')

keyword_keresemdf = df[df.name.str.contains('keres')]
keresemdf = pd.concat([keresemdf, keyword_keresemdf])

 #%% droping the above collected groups fromthe df 

index_names_keresem = df[ df['price'] == 'Keresem' ].index
index_names_ingyenes = df[ df['price'] == 'Ingyenes' ].index
index_names_csere = df[ df['price'] == 'Csere' ].index

df.drop(index_names_keresem, inplace = True)
df.drop(index_names_ingyenes, inplace = True)
df.drop(index_names_csere, inplace = True)

index_name_x=df[df['price'] ==''].index
df.drop(index_name_x, inplace = True)
index_names_keywordkeresem = df[df.name.str.contains('keres')].index
df.drop(index_names_keywordkeresem, inplace = True)
 
#%% Creating numbers from strings 

df['price']=df['price'].str.replace(r'[^0-9]+', '')
df['price']=df['price'].astype(float)
#%%  #%% droping falsely priced items 
 
index_name_toosmall=df[df['price'] <=10000].index
index_name_toobig=df[df['price'] >=2000000].index

# drop these row indexes from dataFrame

df.drop(index_name_toosmall, inplace = True)
df.drop(index_name_toobig, inplace = True)

 #%% creating the enchmark_index
df['benchmark2_index'] = (df['benchmark2']/df['price'])*1000

#%% outlier handling - Zscore
df = df[df['finalmatch2'].notna()]
from scipy import stats
df_outliertest = df.loc[df.groupby(['finalmatch2'])['benchmark2_index'].transform(stats.zscore).abs() > 4]
df.drop(df_outliertest.index, inplace = True)
#df = df[df.groupby(['finalmatch2'])['benchmark2_index'].transform(stats.zscore).abs() < 4]

 #%% colleting suspicous cards - based on the index value 
df = df[df['finalmatch2'].notna()]
df_susgood = df.loc[df['benchmark2_index']>2.5]
df_susbad = df[df['benchmark2_index']<0.05]

nullak = df.loc[df['finalmatch2']==0]
print(sum(pd.isnull(df['finalmatch2'])))

#%% droping nas/label encoding
#df = df[df['finalmatch2'].notna()]
#df = df[df['benchmark2_index']<2.5]
#df = df[df['benchmark2_index']>0.05]

#nullak = df.loc[df['finalmatch2']==0]
#print(sum(pd.isnull(df['finalmatch2'])))


#%% seeking median
df_benchmarkmedians = df.groupby('benchmark2')['benchmark2_index'].aggregate('median')
dicttionary_median = df_benchmarkmedians.to_dict()

#%% creating recos based on the median 
df['benchmarkmedian'] = df['benchmark2'].map(dicttionary_median)
df['premium?'] = np.where(df['benchmark2_index'] > df['benchmarkmedian'], 'Ajánlott', '')

#%% 
okdf = df.loc[df['benchmark2_index'] > df['benchmarkmedian']]

#%%  seeking the q4 
df_quant=df.groupby('benchmark2', as_index=False).quantile([0.75])
dicttionary_quant = dict(zip(df_quant.benchmark2, df_quant.benchmark2_index))
#%% detecting premiums
df['benchmarkq4'] = df['benchmark2'].map(dicttionary_quant)
df['verypremium?'] = np.where(df['benchmark2_index'] > df['benchmarkq4'], 'NagyonAjánlott', '')


 #%% - ordering recos
 
name = np.array(df['name'].str.lower())
matcher = np.array(df['verypremium?'])
matcher2 = np.array(df['premium?'])
recomatch = []      
                
counter = 0
for i in range(len(name)):
    m = matcher[i]
    m2= matcher2[i]
    if m !='':
        recomatch.append(m)
        print(m)
    else: 
        recomatch.append(m2)
        print(m2)

df['recomatch']=recomatch

#%% removing unnecessary columns 
df =df.drop(columns=['premium?', 'verypremium?','damagecheck', 'multicheck' ])

#%% safe save

now = (dt.datetime.now())
date_time = now.strftime("%m_%d_%Y")
df.to_excel(f'df_{date_time}.xlsx', engine='xlsxwriter')

#%% collecting freshly uploaded and sold items 

nonsold = df[df['link'].isin(old_df['link'])]
#nr of entries that are in findable in yesterdays and todays databases
new_df = df[~df['link'].isin(old_df['link'])]
#nr of entries that are in findable only in todays database
sold_df = old_df[~old_df['link'].isin(df['link'])]

#%% removing promoted items from the sold ones

sold_df = sold_df[sold_df['frozen_or_promoted']!='paid']
#nr of entries that are in findable only in yesterday's database

#%% 
#creating new df from the parameters which are calculated 

c = df['finalmatch2'].mode()

calculated_df = pd.DataFrame(columns = ['Date', 'AVG_Price', 'New_Cards', 'Most Common Card'])
calculated_df['AVG_Price'] =  df[["price"]].mean()
calculated_df['New_Cards'] = new_df['name'].count().astype(int)
calculated_df['Sold_Cards'] = sold_df["name"].count()
calculated_df['Most Common Card']=c[0]
calculated_df.Date = (dt.datetime.now())

    
#%% uploading the data 
df_uploader(df,worksheet_name='decupdate')
df_uploader(ingyendf,worksheet_name='ingyen')
df_uploader(keresemdf,worksheet_name='keresem')
df_uploader(cseredf,worksheet_name='csere')
df_uploader(cseredf,worksheet_name='csere')
df_uploader(new_df,worksheet_name='fresh')
df_uploader(sold_df,worksheet_name='sold')
df_uploader_append(calculated_df,worksheet_name='calculated')
    

