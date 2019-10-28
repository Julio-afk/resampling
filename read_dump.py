import pandas as pd
import numpy as np
from pyspark.sql.functions import *
from get_days_vector import *

def read_dump(fecha, spark, sensibilities_table):
    dump_eq_pricing = spark.read.parquet('/data/master/cib/xdip/data/t_xdip_risk_dump_eq_pricing')
    
    fecha_max = str(dump_eq_pricing.agg(max('odate_date')).toPandas().iloc[0,0])
    
    dump_eq_atributes = spark.read.parquet('/data/master/cib/xdip/data/t_xdip_risk_dump_eq_atributes')
    dump_ir_pricing = spark.read.parquet('/data/master/cib/xdip/data/t_xdip_risk_dump_ir_pricing/odate_date='+fecha_max)
    dump_ir_atributes = spark.read.parquet('/data/master/cib/xdip/data/t_xdip_risk_dump_ir_atributes/odate_date='+fecha_max)
    dump_fx_pricing = spark.read.parquet('/data/master/cib/xdip/data/t_xdip_risk_dump_fx_pricing')
    dump_iv_pricing = spark.read.parquet('/data/master/cib/xdip/data/t_xdip_risk_dump_iv_pricing')
    dump_iv_atributes = spark.read.parquet('/data/master/cib/xdip/data/t_xdip_risk_dump_iv_atributes')
    dump_iv_pricing = dump_iv_pricing.filter(col('odate_date') ==fecha_max)
    # dump_ir_pricing = dump_ir_pricing.filter(col('odate_date') ==fecha_max)
    dump_iv_atributes = dump_iv_atributes.filter(col('odate_date') ==fecha_max)
    # dump_ir_atributes = dump_ir_atributes.filter(col('odate_date') ==fecha_max)
    dump_fx = dump_fx_pricing.filter(col('odate_date') ==fecha_max)
    dump_eq = dump_eq_pricing.filter(col('odate_date') ==fecha_max)
    dump_iv = dump_iv_pricing.join(broadcast(dump_iv_atributes), ['full_name','odate_date', 'options_terminal_id']).filter(col('moneyness_number')!=0)
    dump_ir = dump_ir_pricing.join(broadcast(dump_ir_atributes.withColumnRenamed('terms_validity_days_type', 'options_terminal_id')), ['full_name', 'options_terminal_id'])
    rf_names = ['EQ_'+x if 'FX_'not in x else x for x in sensibilities_table.columns]
    dump_inst_s = spark.createDataFrame(pd.DataFrame({'full_name':rf_names, 'section_corep_id':np.nan, 'options_buckets_desc':np.nan}))
    condition_ir = dump_ir.select('full_name', 'date_date', 'largest_amount', 'section_corep_id', 'value_amount').filter(col('year_number')>=2008).\
    join(broadcast(dump_inst_s), ['full_name','section_corep_id'], 'inner')
    condition_ir = condition_ir.withColumn('nombre_completo', regexp_replace('full_name', '[^\w]', '_'))
    condition_ir = condition_ir.withColumn('nombre_completo', concat(col("nombre_completo"), lit("_"), col("section_corep_id")))
    #obtenemos datos del dump de IV
    condition_iv = dump_iv.select('full_name', 'date_date', 'largest_amount', 'options_buckets_desc', 'value_amount').filter(col('year_number')>=2008).\
    join(broadcast(dump_inst_s), ['full_name','options_buckets_desc'], 'inner')
    condition_iv = condition_iv.withColumn('nombre_completo', regexp_replace('full_name', '[^\w]', '_'))
    condition_iv = condition_iv.withColumn('nombre_completo', concat(col("nombre_completo"), lit("_"), col("options_buckets_desc")))
    #obtenemos datos de los dumps de EQ y FX 
    condition_fx = dump_fx.select('full_name', 'date_date', 'variation_risk_per', 'value_amount').withColumnRenamed('variation_risk_per', 'largest_amount')\
    .filter(col('year_number')>=2008).join(broadcast(dump_inst_s), ['full_name'], 'inner')
    condition_eq = dump_eq.select('full_name', 'date_date', 'variation_risk_per', 'value_amount').withColumnRenamed('variation_risk_per', 'largest_amount')\
    .filter(col('year_number')>=2008).join(broadcast(dump_inst_s), ['full_name'], 'inner')
    prices_eq = condition_eq.toPandas()
    prices_ir = condition_ir.toPandas()
    prices_fx = condition_fx.toPandas()
    prices_iv = condition_iv.toPandas()
    instrumentos_liquidos = pd.concat([prices_eq, prices_ir, prices_fx, prices_iv], sort=False)
    instrumentos_liquidos.loc[:,['largest_amount', 'value_amount']] = instrumentos_liquidos.loc[:,['largest_amount', 'value_amount']].apply(lambda x: x.astype(float))
    instrumentos_liquidos['rend'] = instrumentos_liquidos.largest_amount
    instrumentos_liquidos.loc[instrumentos_liquidos.full_name.str.startswith('IR'), 'rend']= instrumentos_liquidos.loc[instrumentos_liquidos.full_name.str.startswith('IR'),'largest_amount']
    precios = instrumentos_liquidos.pivot(columns='full_name', index='date_date', values='value_amount')
    rend = instrumentos_liquidos.pivot(columns='full_name', index='date_date', values='rend')
    precios.columns  = precios.columns.str.replace(' IND', '')
    rend.columns  = rend.columns.str.replace(' IND', '')
    del precios.columns.name, precios.index.name, rend.columns.name, rend.index.name
    rend = rend.fillna(1)
    precios = precios.fillna(method='ffill')
    precios.index = precios.index.astype(str)
    rend.index = rend.index.astype(str)
    rend.index.max()
    fechas = get_days_vector(path_cal, spark)
    fechas = fechas[fechas<=fecha]
    rend = rend.loc[rend.index.isin(fechas)]
    rend = rend.drop_duplicates()
    rend = rend.loc[np.logical_and(rend.index >= '2009', rend.index<= fecha)]
    return [precios, rend, fechas]
