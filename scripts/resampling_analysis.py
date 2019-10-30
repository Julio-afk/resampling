import pandas as pd
import numpy as np
import random
from find_sample import * 
from get_days_vector import * 
from get_mtm import *
from resampling import *
from read_dump import *
from plotting import * 
from aportacion_marginal import * 
def resampling_analysis(nodo, tree, sensibilities_table, rend_table, days_of_period, 
                       method_resampling, method_find_sample, iterations, days_to_select, seed =123):
    
    
    #calculate mtm and node sensibility
    mtm1, sens_nodo, TR_clean = get_mtm(nodo, tree, sensibilities_table, rend_table)
    
    ########################2. Based on the Mark to Market values calculated, we select the worst days of the period (the stress period).####
    period_selected, result = find_sample(mtm1, method_find_sample, days_of_period, sens_nodo, nodo)
    ########################3. Apply Resampling to select values of Mark to Market.###########################################
    mtm = np.array(mtm1.squeeze())
    days_selected = []
    mtm_selected = []
    random.seed(seed)
    seeds = [random.randint(1,10000000000) for x in range(iterations)]
    for i in range(iterations):
        ds = resampling(method_resampling, days_of_period, days_to_select, seeds[i])
        days_selected += [ds]
        mtm_selected += [mtm[ds]]
    ########################4. Create new paths based on the MtM values selected.###############################
    mtm_paths = [np.cumsum(x) for x in mtm_selected] 
    mtm_paths = pd.DataFrame(mtm_paths)
    #create plots 
    fig, ax, ax2, ax3 = plotting(mtm1, mtm_paths, result, days_of_period, period_selected, days_to_select, method_resampling,
            method_find_sample, nodo)
    
    ########################4. Importance of each risk factor .###############################
    mtm_each_factor = TR_clean.mul(sens_nodo.iloc[0].astype(float).tolist())
    mtm_each_factor = mtm_each_factor.iloc[period_selected]
    df_aportacion = mtm_each_factor.apply(lambda x: aportacion_marginal(mtm_each_factor,x))
    df_aportacion.index = ['aportacion_var', 'aportacion_ES']
    df_aportacion = df_aportacion.T.sort_values(['aportacion_var', 'aportacion_ES'])
    
    ########################5.Summary table .###############################
    method_used_find_sample = {1:'Value at Risk',
                              2:'Average Variation',
                              3:'Expected Shortfall'}
    
    method_used_resampling = {1:'All Random',
                             2:'Blocks with fixed number of days',
                             3:'Block with random number of days'}
    
    info_output = pd.DataFrame({'name':['Name of the node selected',
                                        'Method used in looking for the stressed period',
                                        'Method used in Resampling',
                                        'Total number of days in this period',
                                        'Number of days in the stressed period',
                                        'Number of days to select with Resampling'],
                                'variable':[nodo,
                                            method_used_find_sample[method_find_sample], 
                                            method_used_resampling[method_resampling],
                                            len(mtm),
                                            days_of_period,
                                            days_to_select]})
    
        ##################################Building results table ############################################
    var_99 = mtm_paths.apply(lambda x: np.percentile(x,1))
    var_95 = mtm_paths.apply(lambda x: np.percentile(x,5))
    es_all = mtm_paths.apply(lambda x: get_ES(x))
    cols_results = (list(range(days_to_select-1,0,-10)) +[0])[::-1]
    index_results = [str(x+1) + 'th day simulated' if x!=0 else str(x+1) + 'st day simulated' for x in cols_results]
    results_table = pd.DataFrame({'VaR 99%':var_99[cols_results],
                                  'VaR 95%':var_95[cols_results],
                                  'ES 97.5%':es_all[cols_results]})
    results_table.index = index_results
    results_table = results_table.apply(lambda x: x.map('{:,.0f}'.format))
    
    ######################################################################################################
    
    
    return [df_aportacion, info_output, fig, mtm_paths, results_table]
# RES = resampling_analysis(nodo, tree, sensibilities_table, rend, days_of_period, 
#                        method_resampling, method_find_sample, iterations, days_to_select, seed =123)
# RES