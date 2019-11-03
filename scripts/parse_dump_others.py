# -*- coding: utf-8 -*-
"""
Created on Sun Nov  3 18:31:06 2019

@author: Julio
"""
import pandas as pd
import numpy as np
import  time 
from gc impor collect

path_casa = 'C:\projects\data\TEPR_ASE_dump_RgoM_es_Wed.csv'


def create_df(factor_list,  col_names):
    txt = pd.DataFrame([x.split(',') for x in factor_list])
    txt.columns = txt.iloc[0,:].str.replace(r'\(.*?\)', '').tolist()
    txt = txt.reindex(col_names, axis='columns')
    txt = txt.iloc[1:,:]
    txt = txt.replace('',np.nan)
    txt.Value = pd.to_numeric(txt.Value, errors = 'coerce')
    txt.Date = pd.to_datetime(txt.Date, errors = 'coerce')
    txt = txt.dropna(axis='index', how ='all')
    txt.Name = txt.Name.fillna(method  = 'ffill')
    txt = txt.reset_index(drop=True)
    return txt

def parse_dump_others(path, kind):
    
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
    
    col_names = col_names[kind]
    rf_type = rf_type[kind]
    
    s_ini = time.time()
    input_file = open(path)
    text = input_file.readlines()
    s = time.time()
    text = [x[:-1] for x in text ]
    
    chunks = [x for x,y in enumerate(text) if 'BeginDate' in y]
    chunks += [len(text)]
    
    print("time taken: ", time.time()-s)
    
    #get index of headers of IR risk factors
    #    chunks_ir = [x-1 for x,y in enumerate(text) if 'ir_data' in y]
    
    
    chunks_rf =  [x-1 for x,y in enumerate(text) if rf_type in y]
    
    #Create slice to filter risk factors
    slice_rf = [slice(chunks_rf[i],chunks[chunks.index(chunks_rf[i])+1]) for i in range(len(chunks_rf))]
    
    s = time.time()
    text_rf = [text[x] for x in slice_rf]
    print('time with slices: ', time.time()-s)
     
    list_rfs = [create_df(x,  col_names) for x in text_rf]
    
    del text_rf, chunks, text, input_file
    collect()
    
    df_final = pd.concat(list_rfs)
    df_final.Name = df_final.Name.astype('category')
    
    print('time elapsed: ', (time.time() - s_ini)/60 , ' minutes')
    df_final.reset_index(inplace=True, drop=True)
    
    return df_final

data_types = ['mk', 'eq', 'fx', 'com', 'iv']

s = time.time()
mk, eq, fx, com, iv = [parse_dump_others(path_casa, x) for x in data_types]
print('total time elapsed: ', (time.time()-s)/60, ' minutes')



# =============================================================================
# modificando tipos de datos para ahorrar espacio
# =============================================================================
col_terms = ['Moneyness', 'Underlying Term', 'Option Term']

iv.loc[:,col_terms] = iv.loc[:,col_terms].apply(lambda x: x.astype(float))


