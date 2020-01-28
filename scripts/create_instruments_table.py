# -*- coding: utf-8 -*-
"""
Created on Wed Nov 27 15:14:45 2019

@author: e054040
"""
import pandas as pd

path = 'C:/Users/e054040/Desktop/projects/data/20191113/dump/TEPR_ASE_dump_RgoM_es_Wed.csv'

def build_instruments_table(path):
    
    input_file = open(path)
    text = input_file.readlines()
    text = [x[:-1] for x in text ]
    
    chunks = [x for x,y in enumerate(text) if 'BeginDate' in y]
    chunks += [len(text)]
    
    chunks[1]
    slice_list = [slice(chunks[i],chunks[i]+2) for i in range(len(chunks))]
    
    text = [text[x] for x in slice_list]
    text = text[:-1]
    #columnas para hacer el instruments_table
    cols = ['Name(property;string)', 'FactorGroup(property;string)', 'GroupName(property;string)']
    
    #buscamos indice de las columns
    indices = [[j[0].split(',').index(x) for x in cols if x in j[0]] for j in text if len(j) >0]
    
    inst_table = [[x[1].split(',')[j] for j  in m]  for m,x in zip(indices, text) if len(x)>0]
    inst_table  = pd.DataFrame(inst_table)
    inst_table.columns = [ 'Name','FactorGroup', 'GroupName']
    
    return inst_table


#from time import time
#s = time()
#
#inst_table = build_instruments_table(path)
#print('elapsed: ', (time()-s)/60)