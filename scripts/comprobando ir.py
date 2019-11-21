# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 15:35:35 2019

@author: e054040
"""

import os 
import pandas as pd
path_scripts = 'C:/Users/e054040/Desktop/projects/resampling/scripts/'
os.chdir(path_scripts)
import numpy as np
from get_days_vector import get_days_vector
from parse_dump import parse_dump_ir
from parse_dump_others import parse_dump_others

path_datos = 'C:/Users/e054040/Desktop/projects/data/resampling/'
path = 'C:/Users/e054040/Desktop/projects/data/20191113/dump/TEPR_ASE_dump_RgoM_es_Wed.csv'
sens_nodo = pd.read_csv(path_datos + 'sens_nodo/sens_nodo_BBVA_SA.csv')

days_vec = get_days_vector(path_datos)

dump_ir = parse_dump_ir(path)
dump_ir = dump_ir.loc[dump_ir.Date.isin(days_vec)]

dic  = pd.read_csv(path_datos + 'diccionario/diccionario.csv', dtype = {'Term':'float64', 'term_ymd':'str'})
dump_ir = dump_ir.merge(dic, on = 'Term', how='left')



path_nombres = 'C:/Users/e054040/Desktop/projects/data/20191113/names_match/'
names = pd.read_csv(path_nombres + 'names_eqfxiv.csv', index_col=0, header = None, squeeze = True)
# =============================================================================
sens_nodo[['name', 'term']] = sens_nodo.rf.str.split(pat="_(?=[^_]+$)", expand=True)
sens_nodo = sens_nodo.merge(dic, left_on = 'name', right_on = 'sens', how= 'left')
sens_nodo.dump = sens_nodo.dump.fillna(sens_nodo.name)
# =============================================================================
names_ir = dump_ir['Name']+ '_' + dump_ir['term_ymd']

ir = sens_nodo[sens_nodo.rf.str.startswith('IR_')]

# =============================================================================
#seguimos por aqui
ir = sens_nodo[sens_nodo.rf.str.startswith('IR_')]
dict_ir = pd.read_csv('C:/Users/e054040/Desktop/projects/incidencias/dict_delta_ir.csv')
ir[['rf', 'term']] = ir.rf.str.split(pat="_(?=[^_]+$)", expand=True)

ir = ir.merge(dict_ir, on='rf', how = 'left')
ir.dump = ir.dump.fillna(ir.rf)
ir['full_name'] = ir.dump + '_' + ir.term
ir = ir.loc[:,['full_name', 'value']]
ir.full_name = ir.full_name.str.upper()


concat_dump =  dump_ir['Name']+ '_' + dump_ir['term_ymd']

names = ir.full_name.copy()
names = names.str.replace('SEN', 'SENIOR')
names = names.str.replace('BARCLAYS', 'BARCLAYS_BANK')
names = names.str.replace('_COLOMBIA_','_REP_COLOMBIA_')
#names = names.str.replace('_7D', '_1W')



a = names[np.logical_and(~names.isin(concat_dump.str.upper()), ~names.str.lower().str.contains('parallel'))]
len(a)

concat_dump[concat_dump.str.contains('COLOMBIA')]

# =============================================================================

names = names.str.replace('SENIOR', 'SEN')
names = names.str.replace('BARCLAYS_BANK', 'BARCLAYS')
names = names.str.replace('_COLOMBIA_', '_REP_COLOMBIA_')
names = names.str.replace('_7D', '_1W')

#reemplazamos 7D por 1W en ir.rf

ir.rf = ir.rf.str.replace('_7D', '_1W')


a = ir.rf[np.logical_and(~ir.rf.isin(names), ~ir.rf.str.lower().str.contains('parallel'))]
len(a)

dump_ir.Name[dump_ir.Name.str.contains('IR_CDX_HY_S')].unique()
names[names.str.contains('IR_SWAP_ZAR')].unique()
a[a.str.contains('IR_SWAP_COP_O/N')].unique()


ir.loc[ir.rf.str.contains('1W')]

list(set([x[:3] for x in sens_nodo.rf]))

m1 = np.logical_or(sens_nodo.rf.str.startswith('IR_'), sens_nodo.rf.str.startswith('EQ_'))
m2 = np.logical_or(sens_nodo.rf.str.startswith('VOL_'), sens_nodo.rf.str.startswith('FX_'))
m3 = np.logical_or(m1,m2)
j = sens_nodo.loc[~m3]

list(set([x[:3] for x in j.rf]))

eq = parse_dump_others(path, 'mk')

list(set([x[:3] for x in eq.Name]))

from functools import reduce 
def replace_list(serie, txt):
    serie = serie.str.replace(txt, 'EQX_')
    serie = serie.str.replace('__', '_')
    return serie
 
j1 = j.loc[~j.rf.isin(names)]
j2 = reduce(replace_list, [j1.rf]+list(set([x[:4] for x in j1.rf])))

j2 = j2[~j2.isin(eq.Name)]

sens_nodo.loc[j2.index]


