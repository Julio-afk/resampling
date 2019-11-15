import pandas as pd
import numpy as np

def get_mtm(nodo, TS, ST, TR):
    # TS = tree
    # ST = sensitivities_table
    # TR = tb_rendimientos
    # IT = instruments_table
    
    #1. Use the Get.pf.sens function to get the sensitivities of a node.
    sens_nodo = get_pf_sens(nodo, TS, ST)
    TR.columns = TR.columns.str.replace('EQ_', '')
    #2. Clean the node based on if sensitivities are in the IT too.
    sens_nodo = sens_nodo.loc[:,sens_nodo.columns.isin(TR.columns)]
    #3. Get the historical returns of the instruments we have filtered, and replace NA values with 0.
    TR = TR.loc[:,TR.columns.isin(sens_nodo.columns)]
    TR = TR.loc[:,sens_nodo.columns]
    
    print('shape tabla rendimientos: ', TR.shape)
    def na_zero(x):
        x[x.isnull()] = 0
        return x
    sens_nodo_clean = na_zero(sens_nodo)
    TR_clean = na_zero(TR)
    print(sens_nodo.shape[1], ' risk factors in the node ', nodo, '\n')
    
    #4. Calculate the MtM with the historical values and sensitivities. 
    mtm = TR_clean.mul(sens_nodo_clean.iloc[0,:]).sum(axis=1).to_frame(nodo)
    ############################################Weight of each risk factor##############################
#     mtm_each_factor = TR_clean.loc[period_selected].mul(sens_nodo_clean)
#     # mtm_each_factor <- sweep(as.matrix(TR_clean[period_selected,]), MARGIN = 2, as.matrix(sens_nodo_clean), `*`)
#     var_each_factor = mtm_each_factor.apply(lambda x: np.percentile(x,5))
#     es_each_factor = mtm_each_factor.apply(get_ES)
#     list_of_weight = list(var_each_factor, es_each_factor)
    ####################################################################################################
    return([mtm, sens_nodo_clean, TR_clean])
    
def get_ES(vector, probs=2.5):
    var = np.percentile(vector, probs)
    ES = vector[vector<var].mean()
    return ES


def get_pf_sens(nodo, arbol, sensTable, inc_zeroes = False):
    hojas = Getchild(arbol, nodo)
    if isinstance(hojas, str):
        hojas = [hojas]

    if len(hojas) > 0:
        ssl = sensTable.loc[sensTable.port.isin(hojas)]
        ssl = ssl.groupby('rf').agg({'value':'sum'}).reset_index()
        if inc_zeroes:
            return ssl
        else:
            return ssl.loc[ssl.value.abs() >1e-6]
        return '0'
    
    
def Getchild(tree , papa):
    if any(tree.Portfolio==papa):
        leafs=[]
        def Getchild_(tree,papa,rec=0):
            if rec>=20:
                return "Recursion level"
            sons=list(tree.Instrument[tree.Portfolio==papa])
            for j in range(0,len(sons)):
                if any(sons[j] == tree.Portfolio):
                    Getchild_(tree, sons[j], rec+1)
                else:
                    leafs.append(sons[j])
                    
        Getchild_(tree,papa)
        return leafs
    else:
        return papa