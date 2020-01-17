# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 15:02:53 2019

@author: E054040
"""
from gc import collect
import pandas as pd 
import numpy as np


path_datos = 'C:/Users/e054040/Desktop/projects/data/20191113/'

def modify_dump_names(chunk, sens, kind, days_vec, dic, path_datos):
    
#    days_vec = get_days_vector(path_datos)
#    dic  = pd.read_csv(path_datos + 'diccionario/diccionario.csv', dtype = {'Term':'float64', 'term_ymd':'str'})
    chunk = chunk.loc[chunk.Date.isin(days_vec)]
    
    if kind == 'ir': 
        chunk = chunk.merge(dic, on = 'Term', how='left')
        chunk['full_name'] = chunk.Name + '_' + chunk.term_ymd
        chunk = chunk.loc[:,['full_name', 'Date', 'Value']]
#        chunk  = chunk.loc[chunk.full_name.isin(sens.full_name)]
        collect()
    
    ##cargamos dump iv 
    elif kind == 'iv':
        #del VOL_CLP-EUR_365, VOL_CLP-EUR_730
        terms_delete = np.logical_or(chunk.options_terminal_id == '365', chunk.options_terminal_id == '730')
        terms_delete = np.logical_and(chunk.Name == 'VOL_CLP-EUR' , terms_delete)
        chunk = chunk.loc[~terms_delete]
        
        #traducimos options_terminal_id y terms_validity_days_type
        chunk.options_terminal_id = pd.to_numeric(chunk.options_terminal_id, errors= 'coerce')
        chunk.terms_validity_days_type = pd.to_numeric(chunk.terms_validity_days_type, errors= 'coerce')
        
        chunk = chunk.merge(dic, left_on = 'options_terminal_id', right_on = 'Term', how='left')
        chunk = chunk.drop(columns = ['options_terminal_id', 'Term']).rename(columns = {'term_ymd':'options_terminal_id'})
        chunk = chunk.merge(dic, left_on = 'terms_validity_days_type', right_on = 'Term', how='left')
        chunk = chunk.drop(columns = ['terms_validity_days_type', 'Term']).rename(columns = {'term_ymd':'terms_validity_days_type'})
        chunk = chunk.loc[~chunk.options_terminal_id.isnull()]
        
        chunk['full_name'] =  chunk['Name'].astype(str) +'_U'+ chunk['terms_validity_days_type'].map(str)+'_O'+ chunk['options_terminal_id'].map(str) 
        chunk['full_name'] = chunk['full_name'].str.replace('_nan$', '').str.replace('_Unan_O', '_')
        
        #quitamos dos indices con moneyness 0, dejamos moneyness 1 
        chunk = chunk.loc[~np.logical_and(chunk.Name.isin(['VOL_FTSE IND', 'VOL_NIKKEI 225']), chunk.moneyness_number == '0')]
        
#        chunk = chunk.loc[chunk.full_name.isin(sens.full_name), ['full_name', 'Date', 'Value']]
        chunk = chunk.loc[:, ['full_name', 'Date', 'Value']]
        
        print(chunk.shape)
        collect()
    
    elif kind == 'mk':
#        chunk = chunk.loc[chunk.Name.isin(sens.full_name)]
        print(chunk.shape)
        chunk = chunk.rename(columns = {'Name':'full_name'})
    
    elif kind == 'fx':
        #leemos dump fx
#        chunk = chunk.loc[chunk.Name.isin(sens.full_name)]
        chunk = chunk.rename(columns = {'Name':'full_name'})
    
    if chunk.shape[0] == 0:
        return np.nan
    else:
        return chunk



