# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 10:36:26 2019

@author: e054040
"""

import pandas as pd
import numpy as np
import time
path = 'C:/Users/e054040/Desktop/projects/data/20191018/dump/TEPR_ASE_dump_RgoM_es_Thu.csv'
path_casa = 'C:\projects\data\TEPR_ASE_dump_RgoM_es_Wed.csv'
#
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
#
#
#i=0
#df = parse_file(chunks_mk, i, 'mk')
#
#j = [parse_file(chunks_mk, i, 'mk') for i in range(len(chunks_mk))]
#
#df = pd.concat(j)
## =============================================================================
## 
## =============================================================================
def parse_dump_others(path):
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
    
    cols_com = ['Name', 'Date', 'Promptness', 'Value']
    cols_eq_fx_mk = ['Name', 'Date', 'Value']
    cols_iv = ['Name', 'Date', 'Moneyness', 'Option Term', 'Value']
    
    chunks_com =  [x-1 for x,y in enumerate(text) if 'commodity_data' in y]
    chunks_mk =  [x-1 for x,y in enumerate(text) if 'market_data' in y]
    chunks_eq =  [x-1 for x,y in enumerate(text) if 'equity_data' in y]
    chunks_iv =  [x-1 for x,y in enumerate(text) if 'iv_data' in y]
    chunks_fx =  [x-1 for x,y in enumerate(text) if 'fx_data' in y]
    
    #Create slice to filter risk factors
    slice_com = [slice(chunks_com[i],chunks[chunks.index(chunks_com[i])+1]) for i in range(len(chunks_com))]
    slice_mk = [slice(chunks_mk[i],chunks[chunks.index(chunks_mk[i])+1]) for i in range(len(chunks_mk))]
    slice_eq = [slice(chunks_eq[i],chunks[chunks.index(chunks_eq[i])+1]) for i in range(len(chunks_eq))]
    slice_iv = [slice(chunks_iv[i],chunks[chunks.index(chunks_iv[i])+1]) for i in range(len(chunks_iv))]
    slice_fx = [slice(chunks_fx[i],chunks[chunks.index(chunks_fx[i])+1]) for i in range(len(chunks_fx))]
        
    
    s = time.time()
    text_com = [text[x] for x in slice_com]
    text_mk = [text[x] for x in slice_mk]
    text_eq = [text[x] for x in slice_eq]
    text_iv = [text[x] for x in slice_iv]
    text_fx = [text[x] for x in slice_fx]

    print('time with slices: ', time.time()-s)
    
    #Get name of risk factors
    s= time.time()
    nombres_com = [extract_name(x) for x in text_com]
    nombres_mk = [extract_name(x) for x in text_mk]
    nombres_eq = [extract_name(x) for x in text_eq]
    nombres_iv = [extract_name(x) for x in text_iv]
    nombres_fx = [extract_name(x) for x in text_fx]
    
    print('time extracting name: ', time.time()-s)
    
    #sub commas and splitting into list 
    s= time.time()
    txt_com = [split_line(sub_commas(x)) for x in text_com]
    txt_mk = [split_line(sub_commas(x)) for x in text_mk]
    txt_eq = [split_line(sub_commas(x)) for x in text_eq]
    txt_iv = [split_line(sub_commas(x)) for x in text_iv]
    txt_fx = [split_line(sub_commas(x)) for x in text_fx]
    print('time sub commas, split line: ', time.time()-s)
    del text_com, text_mk, text_eq, text_iv, text_fx
    collect()
    
    #Add rf name
    txt_com = add_factor(txt_com, nombres_com)
    txt_mk = add_factor(txt_mk, nombres_mk)
    txt_eq = add_factor(txt_eq, nombres_eq)
    txt_iv = add_factor(txt_iv, nombres_iv)
    txt_fx = add_factor(txt_fx, nombres_fx)
    
    #from [[[]]] to [[]]
    txt_com = [x for v in txt_com for x in v]
    txt_mk = [x for v in txt_mk for x in v]
    txt_eq = [x for v in txt_eq for x in v]
    txt_iv = [x for v in txt_iv for x in v]
    txt_fx = [x for v in txt_fx for x in v]
    
    txt_com = pd.DataFrame(txt_com)
    txt_mk = pd.DataFrame(txt_mk)
    txt_eq = pd.DataFrame(txt_eq)
    txt_iv = pd.DataFrame(txt_iv)
    txt_fx = pd.DataFrame(txt_fx)
    
    txt_com.columns = txt_com.iloc[0,:].str.replace(r'\(.*?\)', '').tolist()
    txt_mk.columns = txt_mk.iloc[0,:].str.replace(r'\(.*?\)', '').tolist()
    txt_eq.columns = txt_eq.iloc[0,:].str.replace(r'\(.*?\)', '').tolist()
    txt_iv.columns = txt_iv.iloc[0,:].str.replace(r'\(.*?\)', '').tolist()
    txt_fx.columns = txt_fx.iloc[0,:].str.replace(r'\(.*?\)', '').tolist()
#    txt.columns = txt.columns.str.replace(r'\(.*?\)', '')
        
    txt_com.Value = pd.to_numeric(txt_com.Value, errors = 'coerce')
    txt_com.Promptness = pd.to_numeric(txt_com.Promptness, errors = 'coerce')
    txt_com.Date = pd.to_datetime(txt_com.Date, errors = 'coerce')
    txt_com = txt_com.dropna(axis='index', how ='all')
    txt_com = txt_com.reset_index(drop=True)
    txt_com.columns = cols_com
    
    txt_mk.Value = pd.to_numeric(txt_mk.Value, errors = 'coerce')
    txt_mk.Date = pd.to_datetime(txt_mk.Date, errors = 'coerce')
    txt_mk = txt_mk.dropna(axis='index', how ='all')
    txt_mk = txt_mk.reset_index(drop=True)
    txt_mk.columns = cols_eq_fx_mk
    
    txt_eq.Value = pd.to_numeric(txt_eq.Value, errors = 'coerce')
    txt_eq.Date = pd.to_datetime(txt_eq.Date, errors = 'coerce')
    txt_eq = txt_eq.dropna(axis='index', how ='all')
    txt_eq = txt_eq.reset_index(drop=True)
    txt_eq.columns = cols_eq_fx_mk
    
    txt_iv.Value = pd.to_numeric(txt_iv.Value, errors = 'coerce')
    txt_iv.Moneyness = pd.to_numeric(txt_iv.Moneyness, errors = 'coerce')
    txt_iv['Option Term'] = pd.to_numeric(txt_iv['Option Term'], errors = 'coerce')
    txt_iv.Date = pd.to_datetime(txt_iv.Date, errors = 'coerce')
    txt_iv = txt_iv.dropna(axis='index', how ='all')
    txt_iv = txt_iv.reset_index(drop=True)
#    txt_iv.columns = cols_iv
    
    txt_fx.Value = pd.to_numeric(txt_fx.Value, errors = 'coerce')
    txt_fx.Date = pd.to_datetime(txt_fx.Date, errors = 'coerce')
    txt_fx = txt_fx.dropna(axis='index', how ='all')
    txt_fx = txt_fx.reset_index(drop=True)
    txt_fx.columns = cols_eq_fx_mk
    
    print('time elapsed: ', (time.time() - s_ini)/60 , ' minutes')
    
    return [txt_com, txt_mk, txt_eq, txt_iv, txt_fx]

txt_com, txt_mk, txt_eq, txt_iv, txt_fx = parse_dump_others(path_casa)

# =============================================================================
# 
# =============================================================================
import re
import time
import pandas as pd
from gc import collect


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

def parse_dump_ir(path):
    s_ini = time.time()
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
    
    txt = pd.DataFrame(txt)
    
    txt.columns = txt.iloc[0,:].tolist()
    txt.columns = txt.columns.str.replace(r'\(.*?\)', '')
        
    txt.Value = pd.to_numeric(txt.Value, errors = 'coerce')
    txt.Term = pd.to_numeric(txt.Term, errors = 'coerce')
    txt.Date = pd.to_datetime(txt.Date, errors = 'coerce')
    txt = txt.dropna(axis='index', how ='all')
    txt = txt.reset_index(drop=True)
    txt.columns = ['Name', 'Date', 'Term','Value']
    print('time elapsed: ', (time.time() - s_ini)/60 , ' minutes')
    return txt
    

txt = parse_dump_ir(path_casa)


