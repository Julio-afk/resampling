import datetime
import pandas as pd 
path_cal = '/data/sandboxes/rskc/data/Risks/opt_rf/data/calendar/TEPR_SAFARI_In_1309CalTARGET.cal'
def get_days_vector(path_cal, spark):
    cal = spark.read.csv(path_cal, header =True).toPandas()
    cal = cal.iloc[:,0].to_frame('days')
    cal.days = (cal.days.str.extract('(.*?) #',expand=False))
    cal = pd.to_datetime(cal.days)
    fechas =  [x for x in  pd.date_range('2008-01-01', '2019-12-31') if x.weekday() not in [5,6]]
    fechas = pd.Series(fechas)[~pd.Series(fechas).isin(cal)].astype(str).reset_index(drop=True)
    return fechas
# fechas = get_days_vector(path_cal, spark)