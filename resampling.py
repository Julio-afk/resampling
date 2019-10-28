import pandas as pd 
import numpy as np
import random 
def resampling(method, total_days, days_to_select, seed):
    random.seed(seed)
    # Method 1:
    if method == 1:
        days_selected = [random.choice(range(total_days)) for x in range(days_to_select)]
        return(days_selected)
    # Method 2:
    if method == 2:
        periods_desired = 10
        if days_to_select % periods_desired != 0:
            print("Please change the number of periods desired so that the number of days could be an integer.")
        else:
            days_of_period =  int(days_to_select/periods_desired)
            initial_days = [random.choice(range(total_days - days_of_period)) for x in range(periods_desired)]
            initial_days = (initial_days*days_of_period)
            initial_days.sort()
            days_to_add = list(range(days_of_period)) * periods_desired
            days_selected = [x+y for x,y in zip(initial_days, days_to_add)]
            return(days_selected)
    # Method 3:
    if method == 3:
        days_selected = []
        while len(days_selected)<=days_to_select:
            random_days = random.choice(range(1,6))
            random_initial = random.choice(range(total_days - random_days))
            days_selected += [random_initial + y for y in range(random_days)]
        return days_selected[: days_to_select]
    
