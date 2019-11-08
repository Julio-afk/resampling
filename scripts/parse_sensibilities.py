# -*- coding: utf-8 -*-
"""
Created on Tue Nov  5 11:36:54 2019

@author: e054040
"""

import pandas as pd 
import os
import gc
import re 


path = 'C:/Users/e054040/Desktop/projects/data/20191018/'

def extract_port_name(col):
    match_name = re.compile('>*(.*?)<>')
    col = col.str.replace('\\*>', '<>')
    col = [re.search(match_name, x).group(1) for x in col]
    return col

def parse_sens(path, kind):
    
    if kind == 'delta':
        sens_files = [x for x in os.listdir(path) if 'Delta01' in x]
    elif kind == 'vega':
        re_vega = re.compile(r'Vega01_[A-Za-z]{2,5}_POS')
        sens_files = list(filter(re_vega.search, os.listdir(path)))

    sens_list = []

    for x in sens_files:
        df = pd.read_csv(path + x, skiprows = 3)
        df = df.rename(columns = {'Unnamed: 0':'port'})
        df.port = extract_port_name(df.port)
        df = df.melt(id_vars = 'port', var_name= 'rf')
        sens_list  += [df]
        del df
        gc.collect()
        
    df = pd.concat(sens_list)
    
    return df
        
df = parse_sens(path, 'vega')


df = df.groupby(['port', 'rf']).agg({'value':'sum'}).reset_index()
df = df.apply(lambda x: x.astype('category') if x.dtypes == object else x)

exp = re.compile('_(.{2,4})_[A-Za-z\d]{2,10}')

a = [re.search(exp, x).group(1) for x in df.rf if re.search(exp, x) != None]

ind = [x for x,y  in enumerate(a) if y == None]

df.iloc[ind]
