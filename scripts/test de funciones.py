
import os 
import pandas as pd
path_scripts = 'C:/Users/e054040/Desktop/projects/resampling/scripts/'
os.chdir(path_scripts)
from parse_dump_others import parse_dump_others
import numpy as np
from get_days_vector import get_days_vector




path_datos = 'C:/Users/e054040/Desktop/projects/data/resampling/'
path = 'C:/Users/e054040/Desktop/projects/data/20191018/dump/TEPR_ASE_dump_RgoM_es_Thu.csv'
sens_nodo = pd.read_csv(path_datos + 'sens_nodo/sens_nodo_BBVA_SA.csv')

days_vec = get_days_vector(path_datos)

#   #dump_eq
#dump_mk = parse_dump_others(path, 'mk')
#eq = sens_nodo[sens_nodo.rf.str.startswith('EQ_')]
#eq = eq.loc[~eq.rf.isin(dump_mk.Name)]
# =============================================================================
 
# #dump_mk
#dump_eq = parse_dump_others(path, 'mk')
##dump_eq.Name = dump_eq.Name.str.replace('EQ_', '')
#eq = sens_nodo[sens_nodo.rf.str.startswith('EQ_')]
#eq.rf[~eq.rf.isin(dump_eq.Name)].unique().tolist()
#eq.rf.unique().tolist()
#len(dump_eq.Name.unique())
# 
# #dump_fx
# dump_fx = parse_dump_others(path, 'fx')
# fx = sens_nodo[sens_nodo.rf.str.startswith('FX_')]
# fx.loc[~fx.rf.isin(dump_fx.Name)]
# =============================================================================

#validando_iv
dump_iv = parse_dump_others(path, 'iv')
dump_iv = dump_iv.loc[dump_iv.Date.isin(days_vec)]
#del VOL_CLP-EUR_365, VOL_CLP-EUR_730
terms_delete = np.logical_or(dump_iv.options_terminal_id == '365', dump_iv.options_terminal_id == '730')
terms_delete = np.logical_and(dump_iv.Name == 'VOL_CLP-EUR' , terms_delete)
dump_iv = dump_iv.loc[~terms_delete]

iv = sens_nodo[sens_nodo.rf.str.startswith('VOL')]
#iv.loc[~iv.rf.isin(dump_iv.Name)]
dump_iv.isnull().sum()

#cargamos diccionario
dic  = pd.read_csv(path_datos + 'diccionario/diccionario.csv', dtype = str)
dump_iv = dump_iv.merge(dic, left_on = 'options_terminal_id', right_on = 'Term', how='left')

dump_iv = dump_iv.drop(columns = ['options_terminal_id', 'Term']).rename(columns = {'term_ymd':'options_terminal_id'})

dump_iv = dump_iv.merge(dic, left_on = 'terms_validity_days_type', right_on = 'Term', how='left')
dump_iv = dump_iv.drop(columns = ['terms_validity_days_type', 'Term']).rename(columns = {'term_ymd':'terms_validity_days_type'})
dump_iv = dump_iv.loc[~dump_iv.options_terminal_id.isnull()]


mask_swptn = ~dump_iv.terms_validity_days_type.isnull()
cols_concat = ['Name','options_terminal_id', 'terms_validity_days_type']

dump_iv['full_name'] =  dump_iv['Name'].astype(str) +'_U'+ dump_iv['terms_validity_days_type'].map(str)+'_O'+ dump_iv['options_terminal_id'].map(str) 
dump_iv['full_name'] = dump_iv['full_name'].str.replace('_nan$', '').str.replace('_Unan_O', '_')
dump_iv.isnull().sum()

#vol_eq
dump_iv.full_name = dump_iv.full_name.str.replace(' 500 IND', '')

dump_iv.full_name = dump_iv.full_name.str.replace('EUROSTOXX50 IND', 'EUROSTOXX50')

dump_iv.full_name = dump_iv.full_name.str.replace('FTSE IND', 'FTSE')

dump_iv.full_name = dump_iv.full_name.str.replace('CAC IND', 'CAC')

dump_iv.full_name = dump_iv.full_name.str.replace('NIKKEI 225', 'NIKKEI')

dump_iv.full_name = dump_iv.full_name.str.replace('OMX INDEX', 'OMX')

dump_iv.full_name = dump_iv.full_name.str.replace('IBEX 35 IND', 'IBEX')

dump_iv.full_name = dump_iv.full_name.str.replace('SSMI IND', 'SMI')

#dump_iv.full_name[dump_iv.full_name.str.contains('SSMI')].unique()


#remove rf with >30% missing dates 
date_filter= dump_iv.groupby(['full_name', 'moneyness_number']).agg({'Date':'count'})
n_days = date_filter.Date.max()
date_filter = date_filter.loc[date_filter.Date <= n_days*0.6].reset_index()
mask_filter = np.logical_and(dump_iv.full_name.isin(date_filter.full_name), dump_iv.moneyness_number.isin(date_filter.moneyness_number))
dump_iv = dump_iv.loc[~mask_filter]


a = iv.loc[np.logical_and(~iv.rf.isin(dump_iv.full_name),~iv.rf.str.lower().str.contains('_parallel'))]
a.rf = a.rf.str.replace('\.1' ,'') 
len(a.rf.unique())
a.sort_values('value')

a.rf[a.rf.str.contains('IBEX')]
dump_iv.full_name[dump_iv.full_name.str.contains('FTSE')].unique()
list(set([x[:-3] for x in a.rf]))

# =============================================================================
# 
# =============================================================================
