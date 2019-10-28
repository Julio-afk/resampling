import pandas as pd 
import numpy as np
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
plt.style.available
plt.style.use('seaborn')
sys.path.insert(0,'/var/sds/homes/E054040/workspace/resampling/')
from find_sample import * 
from get_days_vector import * 
from get_mtm import *
from resampling import *
from read_dump import *
from plotting import * 
from aportacion_marginal import * 
from resampling_analysis import *

ruta_pl_sandbox = '/data/master/risk/market_risk/xmqr/data/t_xmqr_vectors_pl/'
ruta_arbol_port = '/data/master/cib/master_data/xdrd/data/t_xdrd_portfolio_tree/'
ruta_port_dic = '/data/master/cib/master_data/xdrd/data/t_xdrd_risk_portfdic/'
ruta_informe_rv = '/data/sandboxes/rskc/data/Risks/opt_rf/datio/informe_rv/'
ruta_sens_folder = '/data/master/cib/xdip/data/'

fecha_arbol = '20190930'

def load_portfolio_tree(fecha, ruta_arbol_port):
    
    tree = spark.read.parquet(ruta_arbol_port)
    tree = tree.filter(col('odate_date')==fecha).toPandas()
    tree = tree.iloc[:,2:-1]
    tree = tree.apply(lambda x: x.str.replace(':',''))
    tree.columns  = ['Child', 'Parent']
    return tree
tree = load_portfolio_tree(fecha_arbol, ruta_arbol_port)


fecha_datos = '2019-08-30'
fecha_carga_datio = '20190902'
# fecha_datos = '2019-09-30'
# fecha_carga_datio = '20191001'
# nodo = 'ARBITRAGE_H'
nodo = 'MANAGEMENT EQUITY'
days_of_period = 500
method_resampling  = 2
method_find_sample = 2
iterations = 30000
days_to_select = 30

port = spark.read.parquet(ruta_port_dic + '/odate_date=' + fecha_datos.replace('-','')).toPandas()
rent_var = spark.read.csv(ruta_informe_rv + 'informe_rentvar.csv', header=True).toPandas()
def divide_column(df, column):
    s =df[column].str.split('|', expand=True).stack()
    i = s.index.get_level_values(0)
    df2 = df.loc[i].copy()
    df2[column] = s.values
    return df2
rent_var = divide_column(rent_var, 'cod_name_murex')
rent_var.cod_name_murex = rent_var.cod_name_murex.str.strip()
rent_var = rent_var.drop_duplicates()

def read_pl_sandbox(fecha, spark):
    pl = spark.read.parquet(ruta_pl_sandbox)
    pl = pl.filter(col('process_start_date') == fecha).toPandas()
    pl = pl.drop_duplicates().pivot(index = 'portfolio_id', columns = 'operation_date', values = 'day_amount')
    del pl.index.name, pl.columns.name
    return pl
pl = read_pl_sandbox(fecha_datos.replace('-',''), spark)
pl = pl.loc[nodo]

## Cargamos tablas de sensibilidades

port = spark.read.parquet(ruta_port_dic + '/odate_date=' + fecha_datos.replace('-',''))
port = port.select('portfolio_desc', 'basic_unit_operation_desc').withColumnRenamed('basic_unit_operation_desc', 'customer_portfolio_type')
rent_var = spark.read.csv(ruta_informe_rv + 'informe_rentvar.csv', header=True)
rent_var = rent_var.withColumn('cod_name_murex', explode(split('cod_name_murex', '\\|')))
rent_var = rent_var.withColumn('cod_name_murex', trim(col('cod_name_murex')))
rent_var = rent_var.drop_duplicates()
rent_var = rent_var.withColumnRenamed('cod_name_murex', 'security_label_id')
cols_eq_fx = ['customer_portfolio_type', 'cod_name_rw', 'delta_euro_amount']
#delta_eq
delta_eq = spark.read.parquet(ruta_sens_folder + 't_xdip_risk_sens_deltaeq/odate_date=' + fecha_carga_datio)
delta_eq = delta_eq.withColumn('customer_portfolio_type', regexp_replace('customer_portfolio_type','LIS PRESTAMO','LIS PRESTAMOS'))
delta_eq = delta_eq.withColumnRenamed('delta_eq_euro_amount', 'delta_euro_amount')
delta_eq = delta_eq.join(broadcast(rent_var), on='security_label_id')
delta_eq = delta_eq.select(cols_eq_fx)
# delta_fx
delta_fx = spark.read.parquet(ruta_sens_folder + 't_xdip_risk_sens_deltafx/odate_date=' + fecha_carga_datio)
delta_fx = delta_fx.withColumn('customer_portfolio_type', regexp_replace('customer_portfolio_type','LIS PRESTAMO','LIS PRESTAMOS'))
delta_fx = delta_fx.withColumnRenamed('delta_fx_euro_amount', 'delta_euro_amount')
delta_fx = delta_fx.withColumn('cod_name_rw', concat(lit('FX_'),col('currency_id')))
delta_fx = delta_fx.select(cols_eq_fx)
delta = delta_eq.union(delta_fx)
delta = delta.join(broadcast(port), on='customer_portfolio_type')
sens_nodo = delta.filter(col('portfolio_desc') == nodo)
sens_nodo = sens_nodo.filter(abs(col('delta_euro_amount')) >= 1e-6).toPandas()
sens_nodo = sens_nodo.groupby(['portfolio_desc','cod_name_rw']).agg({'delta_euro_amount':'sum'})

## Filtramos por el nodo seleccionado
sensibilities_table = sens_nodo.reset_index().pivot(columns = 'cod_name_rw', index = 'portfolio_desc', values = 'delta_euro_amount')
sensibilities_table = sensibilities_table.loc[:,sensibilities_table.loc[nodo].abs() >=1e-7]
precios, rend, fechas = read_dump(fecha_datos, spark, sensibilities_table)


##Parametros	
method_find_sample = {1:'Value at Risk',
                      2:'Average Variation',
                      3:'Expected Shortfall'}
method_resampling = {1:'All Random',
                     2:'Blocks with fixed number of days',
                     3:'Block with random number of days'}
# nodo = 'ARBITRAGE_H'
nodo = 'MANAGEMENT EQUITY'
days_of_period = 500
method_find_sample = 1
method_resampling  = 2
iterations = 30000
days_to_select = 30


##Hacemos llamada a la funcion

df_aportacion, info_output, fig, mtm_paths, results_table = resampling_analysis(nodo, tree, sensibilities_table, rend, days_of_period, 
                       method_resampling, method_find_sample, iterations, days_to_select, seed =123)
					   

					   