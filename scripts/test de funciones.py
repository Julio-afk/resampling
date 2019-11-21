
import os 
import pandas as pd
path_scripts = 'C:/Users/e054040/Desktop/projects/resampling/scripts/'
os.chdir(path_scripts)
from parse_dump_others import parse_dump_others
import numpy as np
from get_days_vector import get_days_vector




path_datos = 'C:/Users/e054040/Desktop/projects/data/resampling/'
path = 'C:/Users/e054040/Desktop/projects/data/20191113/dump/TEPR_ASE_dump_RgoM_es_Wed.csv'
sens_nodo = pd.read_csv('C:/Users/e054040/Desktop/projects/data/20191113/sens_nodo/sens_nodo_BBVA_SA.csv', index_col=0)
days_vec = get_days_vector(path_datos)

   #dump_eq
dump_mk = parse_dump_others(path, 'mk')
eq = sens_nodo[sens_nodo.rf.str.startswith('EQ_')]
eq = eq.loc[~eq.rf.isin(dump_mk.Name)]

names_eq = dump_mk.Name
print("dim antes: ", sens_nodo.shape)
sens_nodo = sens_nodo[~sens_nodo.rf.isin(names_eq)]
print("dim despues: ", sens_nodo.shape)
# =============================================================================
 #dump_fx
dump_fx = parse_dump_others(path, 'fx')
fx = sens_nodo[sens_nodo.rf.str.startswith('FX_')]
fx.loc[~fx.rf.isin(dump_fx.Name)]
names_fx = dump_fx.Name

print("dim antes: ", sens_nodo.shape)
sens_nodo = sens_nodo[~sens_nodo.rf.isin(dump_fx.Name)]
print("dim despues: ", sens_nodo.shape)
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

names_iv =  dump_iv['Name'].astype(str) +'_U'+ dump_iv['terms_validity_days_type'].map(str)+'_O'+ dump_iv['options_terminal_id'].map(str) 
names_iv = names_iv.str.replace('_nan$', '').str.replace('_Unan_O', '_')

names = names_eq.unique().tolist() + names_fx.unique().tolist() + names_iv.unique().tolist()
names = pd.Series(names)


mask_swptn = ~dump_iv.terms_validity_days_type.isnull()
cols_concat = ['Name','options_terminal_id', 'terms_validity_days_type']

# =============================================================================
#leemos dump ir 
from parse_dump import parse_dump_ir
dump_ir = parse_dump_ir(path)
dic_ir  = pd.read_csv(path_datos + 'diccionario/diccionario.csv', dtype = {'Term':'float64', 'term_ymd':'str'})
dump_ir = dump_ir.merge(dic_ir, on = 'Term', how='left')

names_ir = dump_ir['Name']+ '_' + dump_ir['term_ymd']


# =============================================================================
#leer informe vega eq para hacer mapeos
ruta_informe = 'C:/Users/e054040/Desktop/projects/data/20191113/'
dict_vegaeq = pd.read_csv(ruta_informe + 'TEPR_SAFARIOut_Informe_Instrumentos_Equity_Vega_1311.csv', sep=';', header =None)
dict_vegaeq.columns = ['sens_name', 'full_name']
dict_vegaeq['sens_name'] = dict_vegaeq.sens_name.str.replace('Vega01_','VOL_')
#leemos informe instrumentos IR
dict_ir = pd.read_csv(ruta_informe + 'TEPR_SAFARIOut_Informe_Tipo_Interes_1311.csv')
dict_ir = dict_ir.loc[:,['Nombre RW', 'Static Scenario Set' ]].dropna()
dict_ir.columns = ['full_name', 'sens_name']
dict_ir.sens_name.str.replace('Delta01_', 'IR_')
dict_ir['sens_name'] = [x.replace('Delta01_', 'IR_') if y.startswith('IR') else x.replace('Delta01_','') for x,y in zip(dict_ir.sens_name, dict_ir.full_name)]

dic_cred = sens_nodo.loc[sens_nodo.rf.str.contains('IR_EUR_')].loc[:,['rf']]
dic_cred.columns = ['sens_name']
dic_cred[['factor','term']] = dic_cred['sens_name'].str.split(pat="_(?=[^_]+$)", expand=True)
dic_cred['full_name'] = dic_cred.factor + '_SP_'
dic_cred = dic_cred.drop(columns = ['factor', 'term'])

dic_index = sens_nodo.loc[sens_nodo.rf.str.contains(r'^[A-Z]{3}-', regex=True), ['rf']]
dic_index = dic_index.rename(columns = {'rf':'sens_name_index'})
dic_index['full_name_index'] = dic_index.sens_name_index.str.replace(r'^[A-Z]{3}-','EQX_')

dic = dict_ir.append(dict_vegaeq).append(dic_cred)
del dict_ir, dict_vegaeq, dic_cred


# =============================================================================
# 
# =============================================================================
sens_nodo = sens_nodo.loc[~sens_nodo.rf.str.lower().str.contains('parallel')]


sens_nodo[['name', 'term']] = sens_nodo.rf.str.split(pat="_(?=[^_]+$)", expand=True)
sens_nodo = sens_nodo.merge(dic, left_on = 'name', right_on = 'sens_name', how= 'left')
sens_nodo = sens_nodo.merge(dic_index, left_on = 'rf', right_on = 'sens_name_index', how= 'left')
sens_nodo.full_name = sens_nodo.full_name.fillna(sens_nodo.name)
sens_nodo['full_name'] = sens_nodo.full_name + '_' + sens_nodo.term
sens_nodo.full_name = sens_nodo.full_name.fillna(sens_nodo.full_name_index)
sens_nodo.full_name = sens_nodo.full_name.str.replace('_SEN_', '_SENIOR_')



sens_nodo = sens_nodo.loc[~sens_nodo.full_name.isin(names)]
sens_nodo.shape
sens_nodo = sens_nodo.loc[~sens_nodo.full_name.isin(names_ir)]
print("dim despues: ", sens_nodo.shape)

ruta_sp = 'C:/Users/e054040/Desktop/projects/data/20191113/TEPR_SAFARIOut_Informe_Credit_Spread_1311.csv'
informe_credit_spread = pd.read_csv(ruta_sp)
informe_credit_spread = informe_credit_spread.rename(columns = {'Codigo (A.C.)':'codigo_ac', 'Nombre Traducido':'nombre_traducido'})
informe_credit_spread = informe_credit_spread.loc[:,['nombre_traducido', 'codigo_ac']]
informe_credit_spread['codigo_ac'] = informe_credit_spread['codigo_ac'].str.replace(' ', '_')
informe_credit_spread = informe_credit_spread.drop_duplicates()
informe_credit_spread.codigo_ac[informe_credit_spread.codigo_ac.str.contains('PER')]
traduc_manuales = pd.DataFrame({'nombre_traducido':'INTESA', 'codigo_ac':'BANCA_INTESA_SPA'}, index= [0])
informe_credit_spread = informe_credit_spread.append(traduc_manuales).reset_index(drop=True)

for i,j in zip(informe_credit_spread.nombre_traducido, informe_credit_spread.codigo_ac):
    sens_nodo.full_name = sens_nodo.full_name.str.replace(i,j)

sens_nodo = sens_nodo.loc[~sens_nodo.full_name.isin(names)]
sens_nodo.shape
sens_nodo = sens_nodo.loc[~sens_nodo.full_name.isin(names_ir)]
print("dim despues: ", sens_nodo.shape)
# =============================================================================
# 
# =============================================================================

#dump_iv['full_name'] =  dump_iv['Name'].astype(str) +'_U'+ dump_iv['terms_validity_days_type'].map(str)+'_O'+ dump_iv['options_terminal_id'].map(str) 
#dump_iv['full_name'] = dump_iv['full_name'].str.replace('_nan$', '').str.replace('_Unan_O', '_')
#dump_iv.isnull().sum()

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
