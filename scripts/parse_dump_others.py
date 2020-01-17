# -*- coding: utf-8 -*-
"""
Created on Sun Nov  3 18:31:06 2019

@author: Julio
"""
import pandas as pd
import numpy as np
import  time 
from gc import collect
import os 
path_scripts = 'C:/Users/e054040/Desktop/projects/resampling/scripts/'
os.chdir(path_scripts)
from get_days_vector import get_days_vector
from modify_dump_names import modify_dump_names
from parse_dump import extract_name

#path_casa = 'C:\projects\data\TEPR_ASE_dump_RgoM_es_Wed.csv'
path = 'C:/Users/e054040/Desktop/projects/data/20191018/dump/TEPR_ASE_dump_RgoM_es_Thu.csv'
path_datos = 'C:/Users/e054040/Desktop/projects/data/20191113/'


def create_df(factor_list,  col_names, sens, days_vec, dic, kind):
    
    cols_to_rename = {
        'Option Term':'options_terminal_id',
        'Moneyness':'moneyness_number',
        'Underlying Term':'terms_validity_days_type'
        }
#    col_names +=['FactorGroup', 'GroupName']
    
#    asset = sens.full_name.str.replace('_[0-9]{1,2}[A-Z]{1}$', '').str.replace('_[0-9]{1,2}[A-Z]{1}$', '')
    name = extract_name(factor_list)
    
    if ~sens.full_name.str.contains(name).any():
        return np.nan
    
    txt = pd.DataFrame([x.split(',') for x in factor_list])
    txt.columns = txt.iloc[0,:].str.replace(r'\(.*?\)', '').tolist()
    txt = txt.rename(columns = {'Delta':'Moneyness'})
    txt = txt.reindex(col_names, axis='columns')
    txt = txt.rename(columns = cols_to_rename)
    txt = txt.iloc[1:,:]
    txt = txt.replace('',np.nan)
    txt.Value = pd.to_numeric(txt.Value, errors = 'coerce')
    txt.Date = pd.to_datetime(txt.Date, errors = 'coerce')
    txt = txt.dropna(axis='index', how ='all')
    txt.Name = txt.Name.fillna(method  = 'ffill')
    txt = txt.reset_index(drop=True)
    txt = modify_dump_names(txt, sens, kind, days_vec, dic, path_datos)
    return txt

def parse_dump_others(path, sens,  kind):
    
    col_names = {
            'mk':['Name', 'Date', 'Value'], 
            'eq':['Name', 'Date', 'Value'],
            'fx':['Name', 'Date', 'Value'],
            'com': ['Name', 'Date', 'Promptness', 'Value'],
            'iv':['Name', 'Date', 'Moneyness', 'Underlying Term', 'Option Term', 'Value']
                 }
    
    rf_type = {
            'mk':'market_data', 
            'eq':'equity_data',
            'fx':'fx_data',
            'com':'commodity_data',
            'iv':'iv_data'
                 }
    
    days_vec = get_days_vector(path_datos)
    dic  = pd.read_csv(path_datos + 'diccionario/diccionario.csv', dtype = {'Term':'float64', 'term_ymd':'str'})
    
    col_names = col_names[kind]
    rf_type = rf_type[kind]
    
    s_ini = time.time()
    input_file = open(path)
    text = input_file.readlines()
    text = [x[:-1] for x in text ]
    
    chunks = [x for x,y in enumerate(text) if 'BeginDate' in y]
    chunks += [len(text)]
    
    chunks_rf =  [x-1 for x,y in enumerate(text) if rf_type in y]
    
    #Create slice to filter risk factors
    slice_rf = [slice(chunks_rf[i],chunks[chunks.index(chunks_rf[i])+1]) for i in range(len(chunks_rf))]
    
    text_rf = [text[x] for x in slice_rf]
     
    list_rfs = [create_df(x, col_names, sens, days_vec, dic, kind) for x in text_rf]
    list_rfs = [x for x in list_rfs if str(x) !='nan']
    
    del text_rf, chunks, text, input_file
    collect()
    
    df_final = pd.concat(list_rfs)
    df_final.full_name = df_final.full_name.astype('category')
    
    #filter >= 2007-12-31
    mask = df_final.Date >=  pd.to_datetime('2007-12-31')
    df_final = df_final.loc[mask,:]
    print('time elapsed ', kind, ': ', (time.time() - s_ini)/60 , ' minutes')
    
    df_final.reset_index(inplace=True, drop=True)
        
    return df_final

#parse_dump_others(path, sens,  kind)


# =============================================================================
# data_types = ['mk', 'eq', 'fx', 'com', 'iv']
# 
# 
# s = time.time()
# mk, eq, fx, com, iv = [parse_dump_others(path, x) for x in data_types]
# print('total time elapsed: ', (time.time()-s)/60, ' minutes')
# 
# cols_na = ['moneyness_number', 'terms_validity_days_type']
# 
# iv.loc[:,cols_na]  = iv.loc[:,cols_na].fillna(0).apply(lambda x: x.astype(float))
# 
# path_to_save =  'C:/Users/e054040/Desktop/projects/data/20191018/dump/'
# 
# def save_files(files, data_types, path):
#     path += 'parsed/'
#     if not os.path.exists(path):
#         os.makedirs(path)
#     [x.to_csv(path + y + '.csv', index= False) for x,y in zip(files, data_types)]
#     return
# 
# save_files([iv], ['iv'], path_to_save)
# 
# # =============================================================================
# # pruebas
# i = parse_dump_others(path, 'iv')
# 
# j = iv.iloc[:10000,:].pivot_table(index = ['Name', 'Moneyness', 'Underlying Term', 'Option Term'], columns = 'Date', values = 'Value')
# # =============================================================================
# 
# 
# # =============================================================================
# # modificando tipos de datos para ahorrar espacio
# # =============================================================================
# col_terms = ['Moneyness', 'Underlying Term', 'Option Term']
# 
# iv.loc[:,col_terms] = iv.loc[:,col_terms].apply(lambda x: x.astype(float))
# 
# mask = iv.Date >=  pd.to_datetime('2007-12-31')
# 
# 
# 
# =============================================================================
