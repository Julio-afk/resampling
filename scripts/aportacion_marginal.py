import pandas as pd
import numpy as np
import random
from get_mtm import *
def aportacion_marginal(df, column, impacto=0.01, confianza=99):
    increased = df.copy()
    increased.loc[:,column.name] *= (1 + impacto)
    var_increased = np.percentile(increased.sum(axis=1),100 - confianza) / np.percentile(df.sum(axis=1),100- confianza)
    var_increased -=1
    es_increased = get_ES(increased.sum(axis=1)) / get_ES(df.sum(axis=1))
    es_increased -=1
    return pd.Series([var_increased.round(4),es_increased])
