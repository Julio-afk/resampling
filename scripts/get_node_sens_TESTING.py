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

from create_instruments_table import build_instruments_table
inst_table = build_instruments_table(path_dump)

dump.info()

precios = dump.pivot(index='Date', columns = 'full_name', values = 'Value')
precios.info()

