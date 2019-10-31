# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 10:36:26 2019

@author: e054040
"""

import pandas as pd
import numpy as np
import time
path = 'C:/Users/e054040/Desktop/projects/data/20191018/dump/TEPR_ASE_dump_RgoM_es_Thu.csv'

input_file = open(path)
text = input_file.readlines()
s = time.time()
text = [x[:-1] for x in text ]
print("time taken: ", time.time()-s)
chunks = [x for x,y in enumerate(text) if 'BeginDate' in y]
chunks += [len(text)]
chunks_ir = [x-1 for x,y in enumerate(text) if 'ir_data' in y]
cols_ir = ['Name', 'Date', 'Term','Value']

chunks_types = [x for x,y in enumerate(text) if '_data' in y]

s = time.time()
data_types = pd.Series(text)[chunks_types].str.extract('([a-z]{1,10}_data)', expand = False).dropna().unique()
print(time.time() - s)

chunks_com =  [x-1 for x,y in enumerate(text) if 'commodity_data' in y]
chunks_mk =  [x-1 for x,y in enumerate(text) if 'market_data' in y]
chunks_eq =  [x-1 for x,y in enumerate(text) if 'equity_data' in y]
chunks_iv =  [x-1 for x,y in enumerate(text) if 'iv_data' in y]
chunks_fx =  [x-1 for x,y in enumerate(text) if 'fx_data' in y]

a = pd.DataFrame(text[chunks_mk[0]:chunks_mk[0]+3])
a.columns = ['xx']
a= a.xx.str.split(',', expand =True)

cols_com = ['Name', 'Date', 'Promptness', 'Value']
cols_eq_fx_mk = ['Name', 'Date', 'Value']
cols_iv = ['Name', 'Date', 'Moneyness', 'Option Term', 'Value']

# =============================================================================
# 
# =============================================================================
import time 

def parse_file(chunk_file, i, kind):
    global text
    print('############################################################')
    print(i)
    s1 = time.time()
    s = time.time()
    file = pd.DataFrame([np.nan if np.logical_or(x=='',x=='\n') else x for x in x.split(',')] for x in text[chunk_file[i]:chunks[chunks.index(chunk_file[i])+1]])
    print('creacion df: ', time.time()-s)
# =============================================================================
#     s = time.time()
#     del text[chunk_file[i]:chunks[chunks.index(chunk_file[i])+1]]
#     print('deleting: ', time.time()-s)
# =============================================================================
    s = time.time()
    file.columns = file.iloc[0,:]
    file = file.iloc[1:,:]
    file.columns = file.columns.str.replace('\(.*?\)', '').str.replace('\n','')
    file = file.loc[file.Date >='2007/12/31']
    print(file.shape)
    if file.shape[0] ==0:
         return
    
    if '2019' not in file.Date.iloc[-1]:
#        print('deleting ',file.Date.iloc[-1])
        return
    print('limpiando columnas: ',time.time() -s)
    s = time.time()
    
    
    file.Value = file.Value.astype(float)
    if kind == 'ir':
        file = file.loc[:,cols_ir]
    elif kind == 'fx' or kind == 'mk' or kind == 'eq':
        file = file.loc[:,cols_eq_fx_mk]
    elif kind == 'com':
        file = file.loc[:,cols_com]
    elif kind == 'iv':
        file = file.loc[:,cols_iv]

    print('filtering columns: ',time.time()-s)
    s = time.time()
    file.Name = file.Name.fillna(method = 'ffill')
    file.Name = file.Name.astype('category')
    print('fill na and category',time.time()-s)
    s = time.time()
    print('tiempo total :', time.time()-s1)
    return file


i=2130
df = parse_file(chunks_eq, i, 'eq')

j = [parse_file(chunks_eq, i, 'eq') for i in range(len(chunks_eq))]

for i in range(len(chunks_ir)-1,-1,-1):
    print(i)
    df = parse_file(chunks_ir, i)
#    df = df.loc[:,['Name', 'Date', 'Promptness','Value']]
    j += [df]
df = pd.concat(j)
# =============================================================================
# 
# =============================================================================
rf_names =  pd.Series(text)[chunks].str.extract('_data,(.*?),', expand = False)

com = [x for x,y in enumerate(text) if 'commodity_data' in y]
com[1]

df = parse_file(chunks, 0)
df = df.loc[:,['Name', 'Date', 'Promptness','Value']]
df.info()

# =============================================================================
# =============================================================================
# import multiprocessing as mp
# mp.cpu_count()
# pool = mp.Pool(mp.cpu_count()-1)
# results = pool.map(parse_file, [i for i in range(5)])
#  
# pool.close()
# =============================================================================
# =============================================================================
import sys
sys.getsizeof(text)/1024/1024

s = time.time()
range_ir = [range(chunks_ir[i]+1,chunks[chunks.index(chunks_ir[i])+1]) for i in range(len(chunks_ir))]
range_ir = np.array([x for v in range_ir for x in v ])
print(time.time() - s)

pd.Series(text)[range_ir].to_csv('dump_ir.csv')

dump_ir = text[range_ir]
dump_ir = dump_ir.drop(chunks_ir[1:], axis='index')
dump_ir = dump_ir.str.split(',', expand =True)
dump_ir.columns  = dump_ir.loc[chunks_ir[0]]

s = time.time()
input_file = open(path)
text = input_file.readlines()
#text = pd.Series(text)
print('reading and creating a series: ', time.time()-s)

s = time.time()
text = text.loc[range_ir]
print('time elapsed filtering with array: ', time.time()-s)

mask = text.str.count(',') == 6
prueba = text.loc[mask]


s = time.time()
text = text.str.split(',', expand=True)
print('time elapsed creating dataframe: ', time.time()-s)

# =============================================================================
# 
# =============================================================================


# =============================================================================
# a = [text[x][0] for x in range(len(text))]
# a =  [re.sub(r'\(plit_line(.*?\)', '', x) for x in a]
# a = [re.sub(r'\n', '', x) for x in a]
# a = [x.split(',')[-8:] for x in a]
# a = pd.DataFrame(a)
# a = a.drop_duplicates()
# =============================================================================
import re
import time
import pandas as pd


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

def parse_dump(path):
    s_ini = time.time()
    input_file = open(path)
    text = input_file.readlines()
    s = time.time()
    text = [x[:-1] for x in text ]
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
    

txt = parse_dump(path)
