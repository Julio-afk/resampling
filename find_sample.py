import pandas as pd
import numpy as np
import random
def find_sample(mtm, method_to_use_fs, days_of_period, sens_nodo, nodo):
    if method_to_use_fs ==1:
        var = mtm.rolling(days_of_period).apply(lambda x: np.percentile(x,5), raw=False).dropna()
        var = var.reset_index()
        down_range = var[nodo].idxmin()
        up_range = down_range  + days_of_period 
        period = list(range(down_range , up_range))
        var = var.set_index('index').squeeze()
        del var.index.name
        return [period, var]
    
    if method_to_use_fs ==2:
        average_variation = (mtm/sens_nodo.sum(axis=1).tolist()).rolling(days_of_period, center=True).mean().dropna()
        average_variation = average_variation.reset_index()
        down_range = average_variation[nodo].idxmin()
        up_range = down_range  + days_of_period 
        period = list(range(down_range , up_range))
        average_variation = average_variation.set_index('index').squeeze()
        del average_variation.index.name
        return [period, average_variation]
    
    if method_to_use_fs ==3:
        cvar = mtm.rolling(days_of_period).apply(lambda x: get_ES(x), raw=False).dropna()
        cvar = cvar.reset_index()
        down_range = cvar[nodo].idxmin()
        up_range = down_range  + days_of_period 
        period = list(range(down_range , up_range))
        cvar = cvar.set_index('index').squeeze()
        del cvar.index.name
        return [period, cvar]
    
def get_ES(vector, probs=5):
    var = np.percentile(vector, probs)
    ES =vector[vector<var].mean()
    return ES
