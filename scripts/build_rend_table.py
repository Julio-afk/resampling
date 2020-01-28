# -*- coding: utf-8 -*-
"""
Created on Fri Nov 22 12:25:39 2019

@author: e054040
"""

import pandas as pd
import numpy as np

rend_log = pd.DataFrame({'group':['ir_data', 'market_data', 'fx_data', 'CS', 'VOL_EQ', 'VOL_FX', 'VOL_IR', 'VOL_MP']})
rend_log['log'] = False
rend_log.loc[rend_log.group.isin(['market_data','fx_data','VOL_EQ','VOL_FX']),'log']  = True

list(set([x[:8] for x in precios.columns if x.startswith('VOL')]))


inst_table.head()

#limpiamos nombres de instrumentos del dump filtrado para cruzar con inst_table
#eliminamos vencimientos y vértices de los swaption

inst_names = pd.DataFrame(dump.full_name.unique(), columns = ['full_name'])
inst_names['rf_name'] = inst_names['full_name'].str.extract('(.*?)_U[0-9]{1,2}[A-Z]{1,2}_O[0-9]{1,2}[A-Z]{1,2}')[0]
inst_names['rf_name']  = inst_names['rf_name'].fillna(inst_names['full_name'])
inst_names['rf_name']  = inst_names['rf_name'] .str.extract('(.*?)_[0-9]{1,2}[A-Z]{1,2}')[0].fillna(inst_names['rf_name'] )

#comprobacion de si cruzan todos
#pd.Series(inst_names['rf_name'].unique()).isin(inst_table.Name).sum()

inst_names = inst_names.merge(inst_table, left_on = 'rf_name', right_on = 'Name')
inst_names.loc[inst_names['GroupName'] == 'Vacio', 'GroupName'] = np.nan
inst_names['GroupName'] = inst_names['GroupName'].fillna(inst_names.FactorGroup)

inst_names.loc[inst_names['GroupName'] == 'FX', 'GroupName'] = 'fx_data'
inst_names.loc[inst_names['GroupName'] == 'MI', 'GroupName'] = 'market_data'
inst_names.loc[inst_names['GroupName'] == 'iv_data', 'GroupName'] = 'VOL_FX'
inst_names.loc[inst_names['GroupName'] == 'IR', 'GroupName'] = 'ir_data'


inst_names = inst_names.merge(rend_log, left_on = 'GroupName', right_on = 'group')
