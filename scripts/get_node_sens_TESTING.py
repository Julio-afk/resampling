# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 09:37:35 2019

@author: e054040
"""

import os 
import pandas as pd
path_scripts = 'C:/Users/e054040/Desktop/projects/resampling/scripts/'
os.chdir(path_scripts)
from parse_sensibilities import parse_sens
from get_mtm import  get_pf_sens
from parse_dump_others import parse_dump_others
from parse_dump import parse_dump_ir
from get_days_vector import get_days_vector
from gc import collect
from create_dictionary import create_dictionary
import numpy as np


def parse_portfolio_tree(path_datos):
    
    nombre_arbol = os.listdir(path_datos + 'arbol/')[0]
    
    port = open(path_datos + 'arbol/' + nombre_arbol)
    
    arbol = port.readlines()
    position_tree = [x for x,y in enumerate(arbol) if y.startswith('Position')][0]
    arbol = arbol[position_tree:]
    arbol = [x.replace('\n','').replace(':', '').split(',') for x in arbol]
    arbol = pd.DataFrame(arbol, columns =arbol[0]).iloc[1:,2:4]
#    arbol.columns = ['Child', 'Parent']
    return arbol

path_datos = 'C:/Users/e054040/Desktop/projects/data/20191113/'

deltas = parse_sens(path_datos, 'delta')

vegas = parse_sens(path_datos, 'vega')

sens = pd.concat([deltas, vegas])
# 1bp, 1%, parallel, 
sens.rf = sens.rf.str.replace('_Up1%', '').str.replace('_1%', '').str.replace('_1bp', '')

del deltas, vegas

# Lectura de arbol de portfolios
arbol = parse_portfolio_tree(path_datos)
 
sens = get_pf_sens('BBVA SA', arbol, sens)
print('hola')
sens.to_csv(path_datos + 'sens_nodo/sens_nodo_BBVA_SA.csv')

fecha = '20191113'
sens = create_dictionary(sens, fecha)

#leemos y unimos dump
path_dump = path_datos + 'dump/TEPR_ASE_dump_RgoM_es_Wed.csv'

dump_ir = parse_dump_ir(path_dump, sens, path_datos)

dump_iv = parse_dump_others(path_dump, sens,  'iv')
dump_eq = parse_dump_others(path_dump, sens,  'mk')
dump_fx = parse_dump_others(path_dump, sens,  'fx')

dump = pd.concat([dump_ir,dump_iv, dump_eq, dump_fx])



# =============================================================================
# modificando por aqui, ya se incluyo lo de abajo en la funcion 'modify_dump_names'
# =============================================================================



#Parseamos dump ir y leemos diccionario
dic  = pd.read_csv(path_datos + 'diccionario/diccionario.csv', dtype = {'Term':'float64', 'term_ymd':'str'})
days_vec = get_days_vector(path_datos)
dump_ir = parse_dump_ir(path_dump)
dump_ir = dump_ir.loc[dump_ir.Date.isin(days_vec)]
dump_ir = dump_ir.merge(dic, on = 'Term', how='left')
dump_ir['full_name'] = dump_ir.Name + '_' + dump_ir.term_ymd
dump_ir = dump_ir.loc[:,['full_name', 'Date', 'Value']]
dump_ir  = dump_ir.loc[dump_ir.full_name.isin(sens.full_name)]
collect()

##cargamos dump iv 
dump_iv = parse_dump_others(path_dump, 'iv')
dump_iv = dump_iv.loc[dump_iv.Date.isin(days_vec)]
#del VOL_CLP-EUR_365, VOL_CLP-EUR_730
terms_delete = np.logical_or(dump_iv.options_terminal_id == '365', dump_iv.options_terminal_id == '730')
terms_delete = np.logical_and(dump_iv.Name == 'VOL_CLP-EUR' , terms_delete)
dump_iv = dump_iv.loc[~terms_delete]

#traducimos options_terminal_id y terms_validity_days_type
dump_iv.options_terminal_id = pd.to_numeric(dump_iv.options_terminal_id, errors= 'coerce')
dump_iv.terms_validity_days_type = pd.to_numeric(dump_iv.terms_validity_days_type, errors= 'coerce')

dump_iv = dump_iv.merge(dic, left_on = 'options_terminal_id', right_on = 'Term', how='left')
dump_iv = dump_iv.drop(columns = ['options_terminal_id', 'Term']).rename(columns = {'term_ymd':'options_terminal_id'})
dump_iv = dump_iv.merge(dic, left_on = 'terms_validity_days_type', right_on = 'Term', how='left')
dump_iv = dump_iv.drop(columns = ['terms_validity_days_type', 'Term']).rename(columns = {'term_ymd':'terms_validity_days_type'})
dump_iv = dump_iv.loc[~dump_iv.options_terminal_id.isnull()]

dump_iv['full_name'] =  dump_iv['Name'].astype(str) +'_U'+ dump_iv['terms_validity_days_type'].map(str)+'_O'+ dump_iv['options_terminal_id'].map(str) 
dump_iv['full_name'] = dump_iv['full_name'].str.replace('_nan$', '').str.replace('_Unan_O', '_')

#quitamos dos indices con moneyness 0, dejamos moneyness 1 
dump_iv = dump_iv.loc[~np.logical_and(dump_iv.Name.isin(['VOL_FTSE IND', 'VOL_NIKKEI 225']), dump_iv.moneyness_number == '0')]

dump_iv = dump_iv.loc[dump_iv.full_name.isin(sens.full_name), ['full_name', 'Date', 'Value']]
collect()

#leemos dump eq
dump_eq = parse_dump_others(path_dump, 'mk')
dump_eq = dump_eq.loc[dump_eq.Date.isin(days_vec)]
dump_eq = dump_eq.loc[dump_eq.Name.isin(sens.full_name)]
dump_eq = dump_eq.rename(columns = {'Name':'full_name'})

#leemos dump fx
dump_fx = parse_dump_others(path_dump, 'fx')
dump_fx = dump_fx.loc[dump_fx.Date.isin(days_vec)]
dump_fx = dump_fx.loc[dump_fx.Name.isin(sens.full_name)]
dump_fx = dump_fx.rename(columns = {'Name':'full_name'})

precios = pd.concat([dump_eq, dump_fx, dump_iv, dump_ir],sort=False)
precios = precios.drop_duplicates()
precios = precios.pivot(columns = 'full_name', index = 'Date', values = 'Value')
precios = precios.loc[precios.index.isin(days_vec)]

#remove factors with >= 20% NAs
cols_with_na = (precios.isnull().sum() /precios.shape[0])>0.2
precios.loc[:,~cols_with_na].shape
precios.shape




