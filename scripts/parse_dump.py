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

data_types = pd.Series(text)[chunks_types].str.extract('([a-z]{1,10}_data)', expand = False).dropna().unique()
data_types

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


import time 

def parse_file(chunk_file, i, kind):
    global text
    s1 = time.time()
    s = time.time()
    file = pd.DataFrame([np.nan if x=='' else x for x in x.split(',')] for x in text[chunk_file[i]:chunks[chunks.index(chunk_file[i])+1]])
    print('creacion df: ', time.time()-s)
    s = time.time()
    del text[chunk_file[i]:chunks[chunks.index(chunk_file[i])+1]]
    print('deleting: ', time.time()-s)
    s = time.time()
    file.columns = file.iloc[0,:]
    file = file.iloc[1:,:]
    file.columns = file.columns.str.replace('\(.*?\)', '').str.replace('\n','')
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


j = [parse_file(chunks_ir, i) for i in range(len(chunks_ir)-1,-1,-1)]

for i in range(len(chunks_ir)-1,-1,-1):
    print(i)
    df = parse_file(chunks_ir, i)
#    df = df.loc[:,['Name', 'Date', 'Promptness','Value']]
    j += [df]


df = pd.concat(j)

rf_names =  pd.Series(text)[chunks].str.extract('_data,(.*?),', expand = False)

com = [x for x,y in enumerate(text) if 'commodity_data' in y]
com[1]

df = parse_file(chunks, 0)
df = df.loc[:,['Name', 'Date', 'Promptness','Value']]
df.info()

# =============================================================================
import multiprocessing as mp
mp.cpu_count()
pool = mp.Pool(mp.cpu_count()-1)
results = pool.map(parse_file, [i for i in range(5)])
 
pool.close()
# =============================================================================
import sys
from functools import reduce
sys.getsizeof(text)/1024/1024

range_ir = [list(range(chunks_ir[i]+1,chunks[chunks.index(chunks_ir[i])+1])) for i in range(len(chunks_ir))]
range_ir = [x for v in range_ir for x in v ]

pd.Series(text)[range_ir].to_csv('dump_ir.csv')

dump_ir = text[range_ir]
dump_ir = dump_ir.drop(chunks_ir[1:], axis='index')
dump_ir = dump_ir.str.split(',', expand =True)
dump_ir.columns  = dump_ir.loc[chunks_ir[0]]


range_ir = reduce(lambda x,y: x+y, range_ir)
len(range_ir)
del text

x = parse_file(chunks_ir,10)
