# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 11:06:04 2019

@author: e054040
"""

import pandas as pd 
import os

path_datos = 'C:/Users/e054040/Desktop/projects/data/resampling/'


def get_days_vector(path_datos):
    cal_name = os.listdir(path_datos + 'calendario/')[0]
    cal = pd.read_csv(path_datos + 'calendario/' + cal_name,  encoding = 'iso-8859-1')
    cal = cal.iloc[:,0].to_frame('days')
    cal.days = (cal.days.str.extract('(.*?) #',expand=False))
    cal = pd.to_datetime(cal.days)
    fechas =  [x for x in  pd.date_range('2008-01-01', '2019-12-31') if x.weekday() not in [5,6]]
    fechas = pd.Series(fechas)[~pd.Series(fechas).isin(cal)].astype(str).reset_index(drop=True)
    return fechas
