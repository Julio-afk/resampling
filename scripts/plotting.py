import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
def plotting(mtm1, mtm_paths, result, days_of_period, period_selected, days_to_select, method_resampling,
            method_find_sample, nodo):
    
    fig, (ax,ax2, ax3) = plt.subplots(3,1, figsize=(15,25))
   
    data_plot = result.reset_index()
    data_plot.columns = ['fecha', 'nodo']
#     data_plot = result.to_frame().iloc[500:]
    data_plot = result.to_frame()
    data_plot.index = pd.to_datetime(data_plot.index)
    
    #get point of minimum value
    point = data_plot.loc[data_plot[nodo] == data_plot[nodo].min()].iloc[0].to_frame().T
    point.columns = ['Minimum Value']
    
    if method_find_sample == 1:
        plot_title = 'Value at Risk'
    elif method_find_sample == 2:
        plot_title = 'Average Variation'
    elif method_find_sample == 3:
        plot_title = 'Expected Shortfall'
    data_plot.plot(ax=ax)
    
    #localiza en las fechas los años y meses para el formato de presentacion
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_minor_locator(mdates.MonthLocator())
    
    #set major ticks format
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        
    ax = point.plot(ax=ax, color='red',marker='o' ,markersize=5, grid=True)
    ax.set_title('Evolution of ' + plot_title + ' during the period')
    ax.set_ylabel(plot_title)
    ax.set_xlabel('Dates')
    ax.ticklabel_format(axis='y', style ='plain')
    ########################## plot 2 ##########################
    plot2_data = mtm_paths.quantile([0.01,0.05]).T
    plot2_data.columns  = ['VaR 99%', 'VaR 95%']
    
    min_date = mtm1.index[period_selected[0]]
    max_date = mtm1.index[period_selected[-1]]
    plot2_data.plot(ax=ax2, grid=True)
    ax2.set_title('Stress period of '  + str(days_of_period) + ' days, from '+ min_date + ' to ' + max_date) 
    ax2.set_ylabel('P&L (EUROS)')
    ax2.set_xlabel('Days Simulated')
    ax2.ticklabel_format(axis='y', style ='plain')
                
    ########################## plot 3 ##########################
    n_bins = int(np.sqrt(mtm_paths.shape[0]).round())
    mtm_paths.iloc[:,-1].hist(bins = n_bins, ax=ax3)
    
    ax3.set_title('distribution of P&L during the '+str(days_to_select)+'th simulation day')
    ax3.set_ylabel('Number of Scenarios')
    ax3.set_xlabel('P&L (EUROS)')
        
    return [fig, ax, ax2, ax3]
