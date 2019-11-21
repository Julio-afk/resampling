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
 
#Verificación si todas las hojas están contenidas en las tablas de sensibilidades
#pd.Series(hojas)[~pd.Series(hojas).isin(sens.port)]
#hojas = Getchild(arbol, 'BBVA SA')
#sens = sens.loc[sens.port.isin(hojas)]
#sens = sens.loc[sens.value.abs() >= 1e-6]
#sens = sens.groupby('rf').agg({'value':'sum'}).reset_index()

sens = get_pf_sens('BBVA SA', arbol, sens)
print('hola')
sens.to_csv(path_datos + 'sens_nodo/sens_nodo_BBVA_SA.csv')

