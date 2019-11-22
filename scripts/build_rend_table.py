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


