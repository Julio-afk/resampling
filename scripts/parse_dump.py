# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 10:36:26 2019

@author: e054040
"""

import pandas as pd
import time
import re
from gc import collect

import os
path_scripts = 'C:/Users/e054040/Desktop/projects/resampling/scripts/'
os.chdir(path_scripts)
from get_days_vector import get_days_vector
from modify_dump_names import modify_dump_names

path = 'C:/Users/e054040/Desktop/projects/data/20191018/dump/TEPR_ASE_dump_RgoM_es_Thu.csv'
#path_casa = 'C:\projects\data\TEPR_ASE_dump_RgoM_es_Wed.csv'


def extract_name(factor_list):
    position = factor_list[0].split(',').index('Name(property;string)')
    txt = factor_list[1].split(',')[position]
    return txt

def sub_commas(factor_list):
    txt = [re.sub(',{2,10}', ',',x) for x in factor_list]
    return txt

def split_line(factor_list):
    txt = [x.split(',')[-3:] for x in factor_list]
    return txt

def add_factor(factor_list, factor_name):
    txt = [[[x] + y for y in v] for x,v in zip(factor_name, factor_list)]
    return txt 

def parse_dump_ir(path, sens, path_datos):
    s_ini = time.time()
#    asset = sens.full_name.str.replace('_[0-9]{1,2}[A-Z]{1}$', '')
    days_vec = get_days_vector(path_datos)
    dic  = pd.read_csv(path_datos + 'diccionario/diccionario.csv', dtype = {'Term':'float64', 'term_ymd':'str'})
   
    
    input_file = open(path)
    text = input_file.readlines()
    s = time.time()
    text = [x[:-1] for x in text ]
    
    chunks = [x for x,y in enumerate(text) if 'BeginDate' in y]
    chunks += [len(text)]
    
    print("time taken: ", time.time()-s)
    
    #get index of headers of IR risk factors
    chunks_ir = [x-1 for x,y in enumerate(text) if 'ir_data' in y]
    
    #Create slice to filter ir factors
    slice_ir = [slice(chunks_ir[i],chunks[chunks.index(chunks_ir[i])+1]) for i in range(len(chunks_ir))]
    s = time.time()
    text = [text[x] for x in slice_ir]
    print('time with slices: ', time.time()-s)
    
    #Get name of risk factors
    s= time.time()
    nombres = [extract_name(x) for x in text]
    #filter risk factors with sens on seelected node
    text = [y for x,y in zip(nombres, text) if sens.full_name.str.contains(x).any()]
    nombres = [x for x in nombres if sens.full_name.str.contains(x).any()]
    print('time extracting name: ', time.time()-s)
    
    #sub commas and splitting into list 
    s= time.time()
    txt = [split_line(sub_commas(x)) for x in text]
    print('time sub commas, split line: ', time.time()-s)
    del text
    collect()
    
    #Add rf name
    txt = add_factor(txt, nombres)
    
    #from [[[]]] to [[]]
    txt = [x for v in txt for x in v]
#    time creating dataframe
    s= time.time()
    txt = pd.DataFrame(txt)
    print('time creating df: ', time.time()-s)

    txt.columns = txt.iloc[0,:].tolist()
    txt.columns = txt.columns.str.replace(r'\(.*?\)', '')
    
    txt.columns = ['Name', 'Date', 'Term','Value']
    txt.Term = txt.Term = pd.to_numeric(txt.Term, errors = 'coerce')
    txt.Date = pd.to_datetime(txt.Date, errors = 'coerce')

    txt = modify_dump_names(txt, sens, 'ir', days_vec, dic, path_datos)
               
    txt.Value = pd.to_numeric(txt.Value, errors = 'coerce')
    txt = txt.dropna(axis='index', how ='all')
    txt = txt.reset_index(drop=True)
    
    missing = txt.isnull().sum(axis=1)>1
    txt = txt.loc[~missing]
    print('time elapsed: ', (time.time() - s_ini)/60 , ' minutes')
    return txt
    


# =============================================================================
# dict_tenors = pd.read_csv('dict_tenors.csv', sep = ';')
# dict_tenors = dict_tenors.drop_duplicates()
# tenors[~tenors.isin(dict_tenors.iv)]
# 
# dict_tenors = dict_tenors.append(pd.DataFrame({'iv':[2]})).sort_values('iv').reset_index(drop=True)
# dict_tenors.to_csv('diccionario.csv', index=False)
# =============================================================================



#input_file = open(path_casa)
#text = input_file.readlines()
#s = time.time()
#text = [x[:-1] for x in text ]
#print("time taken: ", time.time()-s)
#chunks = [x for x,y in enumerate(text) if 'BeginDate' in y]
#chunks += [len(text)]
#chunks_ir = [x-1 for x,y in enumerate(text) if 'ir_data' in y]
#cols_ir = ['Name', 'Date', 'Term','Value']
#
#chunks_types = [x for x,y in enumerate(text) if '_data' in y]
#
#s = time.time()
#data_types = pd.Series(text)[chunks_types].str.extract('([a-z]{1,10}_data)', expand = False).dropna().unique()
#print(time.time() - s)

#chunks_com =  [x-1 for x,y in enumerate(text) if 'commodity_data' in y]
#chunks_mk =  [x-1 for x,y in enumerate(text) if 'market_data' in y]
#chunks_eq =  [x-1 for x,y in enumerate(text) if 'equity_data' in y]
#chunks_iv =  [x-1 for x,y in enumerate(text) if 'iv_data' in y]
#chunks_fx =  [x-1 for x,y in enumerate(text) if 'fx_data' in y]
#
#a = pd.DataFrame(text[chunks_mk[0]:chunks_mk[0]+3])
#a.columns = ['xx']
#a= a.xx.str.split(',', expand =True)
#
#cols_com = ['Name', 'Date', 'Promptness', 'Value']
#cols_eq_fx_mk = ['Name', 'Date', 'Value']
#cols_iv = ['Name', 'Date', 'Moneyness', 'Option Term', 'Value']
#
## =============================================================================
## 
## =============================================================================
#import time 
#
#def parse_file(chunk_file, i, kind):
#    global text
#    print('############################################################')
#    print(i)
#    s1 = time.time()
#    s = time.time()
#    file = pd.DataFrame([np.nan if np.logical_or(x=='',x=='\n') else x for x in x.split(',')] for x in text[chunk_file[i]:chunks[chunks.index(chunk_file[i])+1]])
#    print('creacion df: ', time.time()-s)
#
#    s = time.time()
#    file.columns = file.iloc[0,:]
#    file = file.iloc[1:,:]
#    file.columns = file.columns.str.replace('\(.*?\)', '').str.replace('\n','')
#    file = file.loc[file.Date >='2007/12/31']
#    print(file.shape)
#    if file.shape[0] ==0:
#         return
#    
#    if '2019' not in file.Date.iloc[-1]:
#        print('deleting ',file.Date.iloc[-1])
#        return
#    print('limpiando columnas: ',time.time() -s)
#    s = time.time()
#    
#    
#    file.Value = file.Value.astype(float)
#    if kind == 'ir':
#        file = file.loc[:,cols_ir]
#    elif kind == 'fx' or kind == 'mk' or kind == 'eq':
#        file = file.loc[:,cols_eq_fx_mk]
#    elif kind == 'com':
#        file = file.loc[:,cols_com]
#    elif kind == 'iv':
#        file = file.loc[:,cols_iv]
#
#    print('filtering columns: ',time.time()-s)
#    s = time.time()
#    file.Name = file.Name.fillna(method = 'ffill')
#    file.Name = file.Name.astype('category')
#    print('fill na and category',time.time()-s)
#    s = time.time()
#    print('tiempo total :', time.time()-s1)
#    return file

# =============================================================================
# 
# =============================================================================