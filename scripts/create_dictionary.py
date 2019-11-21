# -*- coding: utf-8 -*-
"""
Created on Thu Nov 21 13:57:39 2019

@author: e054040
"""
import pandas as pd

def create_dictionary(sens_nodo, fecha):
    """
    Es necesario tener los informes de:
        TEPR_SAFARIOut_Informe_Instrumentos_Equity_Vega_1311
        TEPR_SAFARIOut_Informe_Tipo_Interes_1311
        TEPR_SAFARIOut_Informe_Credit_Spread_1311
    """

    ruta_informe = 'C:/Users/e054040/Desktop/projects/data/' + fecha + '/'
    #leemos informe instrumentos vega_eq
    dict_vegaeq = pd.read_csv(ruta_informe + 'TEPR_SAFARIOut_Informe_Instrumentos_Equity_Vega_1311.csv', sep=';', header =None)
    dict_vegaeq.columns = ['sens_name', 'full_name']
    dict_vegaeq['sens_name'] = dict_vegaeq.sens_name.str.replace('Vega01_','VOL_')
    
    #leemos informe instrumentos IR
    dict_ir = pd.read_csv(ruta_informe + 'TEPR_SAFARIOut_Informe_Tipo_Interes_1311.csv')
    dict_ir = dict_ir.loc[:,['Nombre RW', 'Static Scenario Set' ]].dropna()
    dict_ir.columns = ['full_name', 'sens_name']
    dict_ir.sens_name.str.replace('Delta01_', 'IR_')
    dict_ir['sens_name'] = [x.replace('Delta01_', 'IR_') if y.startswith('IR') else x.replace('Delta01_','') for x,y in zip(dict_ir.sens_name, dict_ir.full_name)]
     
    #creamos diccionario de credito
    dic_cred = sens_nodo.loc[sens_nodo.rf.str.contains('IR_EUR_')].loc[:,['rf']]
    dic_cred.columns = ['sens_name']
    dic_cred[['factor','term']] = dic_cred['sens_name'].str.split(pat="_(?=[^_]+$)", expand=True)
    dic_cred['full_name'] = dic_cred.factor + '_SP'
    dic_cred = dic_cred.drop(columns = ['sens_name', 'term'])
    dic_cred = dic_cred.rename(columns={'factor':'sens_name'})
    dic_cred = dic_cred.drop_duplicates()
    
    dic_index = sens_nodo.loc[sens_nodo.rf.str.contains(r'^[A-Z]{3}-', regex=True), ['rf']]
    dic_index = dic_index.rename(columns = {'rf':'sens_name_index'})
    dic_index['full_name_index'] = dic_index.sens_name_index.str.replace(r'^[A-Z]{3}-','EQX_')
    
    dic = dict_ir.append(dict_vegaeq).append(dic_cred)
    del dict_ir, dict_vegaeq, dic_cred


    sens_nodo = sens_nodo.loc[~sens_nodo.rf.str.lower().str.contains('parallel')]
        
    sens_nodo[['name', 'term']] = sens_nodo.rf.str.split(pat="_(?=[^_]+$)", expand=True)
    sens_nodo = sens_nodo.merge(dic, left_on = 'name', right_on = 'sens_name', how= 'left')
    sens_nodo = sens_nodo.merge(dic_index, left_on = 'rf', right_on = 'sens_name_index', how= 'left')
    sens_nodo.full_name = sens_nodo.full_name.fillna(sens_nodo.name)
    sens_nodo['full_name'] = sens_nodo.full_name + '_' + sens_nodo.term
    sens_nodo.full_name = sens_nodo.full_name.fillna(sens_nodo.full_name_index)
    sens_nodo.full_name = sens_nodo.full_name.str.replace('_SEN_', '_SENIOR_')
    
    informe_credit_spread = pd.read_csv(ruta_informe + 'TEPR_SAFARIOut_Informe_Credit_Spread_1311.csv')
    informe_credit_spread = informe_credit_spread.rename(columns = {'Codigo (A.C.)':'codigo_ac', 'Nombre Traducido':'nombre_traducido'})
    informe_credit_spread = informe_credit_spread.loc[:,['nombre_traducido', 'codigo_ac']]
    informe_credit_spread['codigo_ac'] = informe_credit_spread['codigo_ac'].str.replace(' ', '_')
    informe_credit_spread = informe_credit_spread.drop_duplicates()
    informe_credit_spread.codigo_ac[informe_credit_spread.codigo_ac.str.contains('PER')]
    traduc_manuales = pd.DataFrame({'nombre_traducido':'INTESA', 'codigo_ac':'BANCA_INTESA_SPA'}, index= [0])
    informe_credit_spread = informe_credit_spread.append(traduc_manuales).reset_index(drop=True)
    informe_credit_spread.codigo_ac = informe_credit_spread.codigo_ac.str.replace('LA_CAIXA', 'CAIXA')
    informe_credit_spread = informe_credit_spread.drop_duplicates()
    informe_credit_spread = informe_credit_spread.apply(lambda x: 'IR_'+x)

    
    for i,j in zip(informe_credit_spread.nombre_traducido, informe_credit_spread.codigo_ac):
        sens_nodo.full_name = sens_nodo.full_name.str.replace(i,j)
        
    
    sens_nodo = sens_nodo.loc[:,['rf', 'value', 'full_name']]
    
    return sens_nodo