# -*- coding: utf-8 -*-
"""
Created on Fri Nov  8 11:11:25 2019

@author: e054040
"""


import pandas as pd
import os 
os.chdir('C:/Users/e054040/Desktop/projects/resampling/scripts/')
from parse_dump import parse_dump_ir 

path = 'C:/Users/e054040/Desktop/projects/data/20191018/dump/TEPR_ASE_dump_RgoM_es_Thu.csv'
path_datos = 'C:/Users/e054040/Desktop/projects/data/resampling/'


dump = parse_dump_ir(path)

diccionario = pd.read_csv(path_datos + 'diccionario/diccionario.csv')
 
dump = dump.merge(diccionario, on= 'Term')
dump['full_name'] = dump['Name'] +'_'+ dump['term_ymd']
dump = dump.drop(['term_ymd', 'Name', 'Term'], axis='columns')


#dump = dump.pivot(index= 'Date', columns = 'full_name')

#dump = dump.pivot(index= 'Date', columns = 'full_name')
a = dump.groupby(['full_name','Date']).count()


a = a.Value[a.Value >1]
a.index